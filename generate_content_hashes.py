"""Generate semantic content hashes for message deduplication.

Also generates media hashes for attachment-based deduplication.

Processes messages to create normalized summaries, then hashes them for
cross-channel duplicate detection.

Supports multiple AI providers (Mistral, Cerebras) via the ai_providers module.
"""

import hashlib
import logging
import sys
import time
from pathlib import Path

from config import (
    MISTRAL_API_KEY,
    MISTRAL_MODEL,
    CEREBRAS_API_KEY,
    CEREBRAS_MODEL,
    AI_PROVIDER,
    DEDUP_MESSAGES_PER_RUN,
    DEDUP_MIN_MESSAGE_LENGTH,
    MEDIA_DIR,
    validate_config,
)
from database import Database, DatabaseMigration
from ai_providers import AIProvider, MistralProvider, CerebrasProvider

# Configure logging with UTF-8 encoding for Windows
import io
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))],
)
logger = logging.getLogger(__name__)

# Rate limiting
API_DELAY_SECONDS = 0.5  # Delay between API calls


def get_ai_provider() -> AIProvider | None:
    """Get the configured AI provider.

    Returns the provider based on AI_PROVIDER setting:
    - "mistral": Use Mistral API
    - "cerebras": Use Cerebras API
    - "auto": Use first available (Mistral, then Cerebras)

    Returns None if no provider is configured.
    """
    providers = {
        "mistral": lambda: MistralProvider(MISTRAL_API_KEY, MISTRAL_MODEL),
        "cerebras": lambda: CerebrasProvider(CEREBRAS_API_KEY, CEREBRAS_MODEL),
    }

    if AI_PROVIDER.lower() in providers:
        provider = providers[AI_PROVIDER.lower()]()
        if provider.is_configured():
            return provider
        logger.warning(f"AI provider '{AI_PROVIDER}' selected but not configured")
        return None

    # Auto mode: try providers in order
    if AI_PROVIDER.lower() == "auto":
        for name, create_provider in providers.items():
            provider = create_provider()
            if provider.is_configured():
                logger.info(f"Using AI provider: {provider.name}")
                return provider
        return None

    logger.warning(f"Unknown AI provider: {AI_PROVIDER}")
    return None


def normalize_keywords(keywords_str: str) -> str:
    """Normalize and sort keywords for consistent hashing."""
    # Split by comma, strip whitespace, lowercase, remove empty
    keywords = [k.strip().lower() for k in keywords_str.split(',') if k.strip()]
    # Remove duplicates and sort
    keywords = sorted(set(keywords))
    # Join back
    return ','.join(keywords)


def compute_hash(normalized_text: str) -> str:
    """Generate SHA256 hash from normalized keywords."""
    clean_text = normalize_keywords(normalized_text)
    return hashlib.sha256(clean_text.encode('utf-8')).hexdigest()


def sha256_file(file_path: Path) -> str | None:
    """Compute SHA256 hash of a file's contents."""
    try:
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except (OSError, IOError) as e:
        logger.warning(f"Failed to hash file {file_path}: {e}")
        return None


def generate_media_hashes() -> tuple[int, int, int]:
    """Generate media hashes for attachment-based deduplication.

    For albums (grouped_id), all media files are combined into a single hash.

    Returns:
        Tuple of (processed_count, duplicates_found, skipped_count)
    """
    logger.info("Starting media hash generation...")

    # Get channels with dedup enabled
    with Database() as db:
        channels = [dict(row) for row in db.get_dedup_channels()]

    if not channels:
        logger.info("No channels with dedup enabled")
        return (0, 0, 0)

    total_processed = 0
    total_duplicates = 0
    total_skipped = 0

    # Track processed grouped_ids to avoid duplicate processing within a run
    processed_albums: set[tuple[int, int]] = set()  # (channel_id, grouped_id)

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]
        group_id = channel["group_id"]

        # First, skip messages without media
        with Database() as db:
            no_media_ids = db.get_messages_without_media_for_skip(channel_id, limit=500)
            if no_media_ids:
                for msg_id in no_media_ids:
                    db.skip_media_hash(channel_id, msg_id)
                db.commit()
                total_skipped += len(no_media_ids)

        # Get messages needing media hashes
        with Database() as db:
            messages = db.get_messages_needing_media_hashes(
                channel_id, limit=DEDUP_MESSAGES_PER_RUN
            )

        if not messages:
            continue

        logger.info(f"Processing {len(messages)} media messages from: {channel_title}")

        for msg in messages:
            grouped_id = msg.get("grouped_id")

            # For albums, skip if we already processed this group in this run
            if grouped_id:
                album_key = (channel_id, grouped_id)
                if album_key in processed_albums:
                    continue
                processed_albums.add(album_key)

            # Collect media paths
            if grouped_id:
                # Album: get ALL messages in this group
                with Database() as db:
                    album_msgs = db.get_album_messages(channel_id, grouped_id)
                media_paths = sorted([
                    m["media_path"] for m in album_msgs
                    if m.get("media_path")
                ])
                album_msg_ids = [m["id"] for m in album_msgs]
                msg_date = album_msgs[0]["date"] if album_msgs else msg["date"]
            else:
                # Single message
                media_paths = [msg["media_path"]] if msg.get("media_path") else []
                album_msg_ids = [msg["id"]]
                msg_date = msg["date"]

            if not media_paths:
                # No media to hash
                with Database() as db:
                    for msg_id in album_msg_ids:
                        db.skip_media_hash(channel_id, msg_id)
                    db.commit()
                total_skipped += len(album_msg_ids)
                continue

            # Hash each file
            file_hashes = []
            all_files_exist = True
            for rel_path in media_paths:
                full_path = MEDIA_DIR / rel_path
                if full_path.exists():
                    file_hash = sha256_file(full_path)
                    if file_hash:
                        file_hashes.append(file_hash)
                    else:
                        all_files_exist = False
                        break
                else:
                    all_files_exist = False
                    break

            if not file_hashes or not all_files_exist:
                # Can't hash - skip for now (maybe files not downloaded yet)
                continue

            # Combine hashes (sorted paths ensure consistent order)
            combined = ''.join(file_hashes)
            media_hash = hashlib.sha256(combined.encode('utf-8')).hexdigest()

            # Check for duplicate and register
            with Database() as db:
                original = db.register_media_hash(
                    media_hash, channel_id, album_msg_ids[0], msg_date,
                    group_id=group_id
                )

                if original:
                    # This is a duplicate - mark ALL album messages
                    for msg_id in album_msg_ids:
                        db.mark_as_duplicate(channel_id, msg_id, original[0], original[1])
                        db.update_media_hash(channel_id, msg_id, media_hash)
                    total_duplicates += 1
                    logger.info(f"  Duplicate (media): msgs {album_msg_ids} -> channel {original[0]} msg {original[1]}")
                else:
                    # First occurrence - update all album messages
                    for msg_id in album_msg_ids:
                        db.update_media_hash(channel_id, msg_id, media_hash)

                db.commit()

            total_processed += 1

    return (total_processed, total_duplicates, total_skipped)


def generate_text_hashes(ai_provider: AIProvider) -> tuple[int, int, int, int]:
    """Generate AI-based content hashes for text deduplication.

    Skips messages already marked as duplicates (e.g., from media hashing).

    Returns:
        Tuple of (processed_count, duplicates_found, skipped_count, error_count)
    """
    logger.info(f"Starting text hash generation using {ai_provider.name}...")

    total_processed = 0
    total_duplicates = 0
    total_skipped = 0
    total_errors = 0

    # Get channels with dedup enabled
    with Database() as db:
        channels = [dict(row) for row in db.get_dedup_channels()]
        exclusion_groups = db.get_all_tag_exclusions()

    if exclusion_groups:
        logger.info(f"Loaded {len(exclusion_groups)} tag exclusion groups")

    if not channels:
        logger.info("No channels with dedup enabled")
        return (0, 0, 0, 0)

    logger.info(f"Found {len(channels)} active channels for text deduplication")

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]
        group_id = channel["group_id"]

        # First, skip messages that are too short
        with Database() as db:
            short_ids = db.get_short_messages_for_skip(
                channel_id, limit=500, min_length=DEDUP_MIN_MESSAGE_LENGTH
            )
            if short_ids:
                for msg_id in short_ids:
                    db.skip_content_hash(channel_id, msg_id)
                db.commit()
                total_skipped += len(short_ids)
                logger.info(f"  {channel_title}: Skipped {len(short_ids)} short messages")

        # Get messages needing hashes
        with Database() as db:
            messages = db.get_messages_needing_hashes(
                channel_id, limit=DEDUP_MESSAGES_PER_RUN, min_length=DEDUP_MIN_MESSAGE_LENGTH
            )

        if not messages:
            continue

        logger.info(f"Processing {len(messages)} messages from: {channel_title}")

        for msg in messages:
            # Check if already marked as duplicate (e.g., from media hashing)
            with Database() as db:
                full_msg = db.get_message(channel_id, msg["id"])
                if full_msg and full_msg.get("duplicate_of_channel"):
                    # Already a duplicate, skip AI processing but mark content hash as done
                    db.skip_content_hash(channel_id, msg["id"])
                    db.commit()
                    total_skipped += 1
                    continue

            message_text = msg["message"]

            # Rate limit
            time.sleep(API_DELAY_SECONDS)

            # Generate AI summary via provider
            ai_summary = ai_provider.generate_summary(message_text)

            if not ai_summary:
                logger.warning(f"  Failed to get summary for message {msg['id']}")
                total_errors += 1
                continue

            # Treat very short output (less than 3 tokens) as empty
            tokens = [t for t in ai_summary.strip().split(',') if t.strip()]
            if len(tokens) < 3:
                logger.info(f"  Skipping message {msg['id']}: AI returned too few tokens ({len(tokens)}): {ai_summary.strip()}")
                with Database() as db:
                    db.skip_content_hash(channel_id, msg["id"])
                    db.commit()
                total_skipped += 1
                continue

            # Skip promotional content
            if ai_summary.strip().lower() == "ad":
                with Database() as db:
                    db.skip_content_hash(channel_id, msg["id"])
                    db.commit()
                total_skipped += 1
                continue

            # Compute hash from the AI summary
            content_hash = compute_hash(ai_summary)

            # Check tag exclusions - auto-mark as read if matched
            if exclusion_groups and Database.check_tag_exclusions(ai_summary, exclusion_groups):
                with Database() as db:
                    now = int(time.time())
                    cursor = db.cursor()
                    cursor.execute(
                        f"UPDATE channel_{channel_id} SET read = 1, read_at = ? WHERE id = ? AND read = 0",
                        (now, msg["id"])
                    )
                    db.update_content_hash(channel_id, msg["id"], content_hash, ai_summary)
                    db.commit()
                logger.info(f"  Auto-excluded (tag match): msg {msg['id']}")
                total_skipped += 1
                continue

            # Check for duplicates and register
            with Database() as db:
                original = db.register_content_hash(
                    content_hash, channel_id, msg["id"], msg["date"],
                    group_id=group_id
                )

                if original:
                    # This is a duplicate
                    db.mark_as_duplicate(channel_id, msg["id"], original[0], original[1])
                    db.update_content_hash(channel_id, msg["id"], content_hash, ai_summary)
                    total_duplicates += 1
                    logger.info(f"  Duplicate: msg {msg['id']} -> channel {original[0]} msg {original[1]}")
                    logger.info(f"    Summary: {ai_summary[:100]}...")
                else:
                    # First occurrence
                    db.update_content_hash(channel_id, msg["id"], content_hash, ai_summary)

                db.commit()

            total_processed += 1

    return (total_processed, total_duplicates, total_skipped, total_errors)


def generate_content_hashes() -> None:
    """Main function to generate content hashes for deduplication.

    Order of processing:
    1. Media hashes (fast, no API calls)
    2. Text hashes via AI (slower, requires API)

    Messages already marked as duplicates from media hashing are skipped
    for text hashing to save API calls.
    """
    logger.info("Starting deduplication hash generation...")

    validate_config()

    # Run migrations to ensure columns exist
    DatabaseMigration().migrate()

    # Step 1: Media hashing (fast, no API needed)
    media_processed, media_duplicates, media_skipped = generate_media_hashes()

    logger.info("=" * 50)
    logger.info("Media hash generation completed!")
    logger.info(f"  Processed: {media_processed}")
    logger.info(f"  Duplicates found (media): {media_duplicates}")
    logger.info(f"  Skipped (no media): {media_skipped}")
    logger.info("=" * 50)

    # Step 2: Text hashing via AI (slower, requires API)
    ai_provider = get_ai_provider()
    if not ai_provider:
        logger.warning("No AI provider configured - skipping text-based deduplication")
        return

    text_processed, text_duplicates, text_skipped, text_errors = generate_text_hashes(ai_provider)

    logger.info("=" * 50)
    logger.info("Text hash generation completed!")
    logger.info(f"  Processed: {text_processed}")
    logger.info(f"  Duplicates found (text): {text_duplicates}")
    logger.info(f"  Skipped (short/promo/already dup): {text_skipped}")
    logger.info(f"  Errors: {text_errors}")
    logger.info("=" * 50)


if __name__ == "__main__":
    generate_content_hashes()
