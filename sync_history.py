"""Download historical messages for channels with download_all enabled.

Uses tg_daemon if available, falls back to direct Telethon connection.
"""

import asyncio
import hashlib
import json
import logging
import sys
from pathlib import Path

from config import MEDIA_DIR
from database import Database, DatabaseMigration
from tg_client import TGClient, TGClientConnectionError, TGFloodWaitError, is_daemon_running

# Hash constants (same as tg_daemon.py)
HASH_SIZE_THRESHOLD = 64 * 1024  # 64KB - files <= this size are matched by size only
HASH_CHUNK_SIZE = 64 * 1024  # Hash first 64KB of larger files

# Backup folder subfolders to scan
BACKUP_SUBFOLDERS = ["photos", "files", "video_files"]


def compute_file_hash(file_path: Path) -> str | None:
    """Compute MD5 hash of first 64KB of a file.

    For files <= 64KB, returns None (use size matching instead).
    For files > 64KB, returns MD5 hash of first 64KB.
    """
    try:
        file_size = file_path.stat().st_size
        if file_size <= HASH_SIZE_THRESHOLD:
            return None  # Small files don't need hash

        with open(file_path, "rb") as f:
            chunk = f.read(HASH_CHUNK_SIZE)
            return hashlib.md5(chunk).hexdigest()
    except Exception:
        return None


def scan_backup_folder(backup_path: str) -> list[tuple[str, int, str | None]]:
    """Scan a backup folder and compute hashes for all files.

    Scans subfolders: photos, files, video_files

    Returns list of (file_path, file_size, hash) tuples.
    Hash is None for files <= 64KB.
    """
    backup_dir = Path(backup_path)
    if not backup_dir.exists():
        return []

    results = []
    for subfolder in BACKUP_SUBFOLDERS:
        folder_path = backup_dir / subfolder
        if not folder_path.exists():
            continue

        for file_path in folder_path.rglob("*"):
            if not file_path.is_file():
                continue

            try:
                file_size = file_path.stat().st_size
                file_hash = compute_file_hash(file_path)
                results.append((str(file_path), file_size, file_hash))
            except Exception as e:
                logger.warning(f"Error scanning {file_path}: {e}")

    return results


def update_backup_hashes(channel_id: int, backup_path: str) -> int:
    """Scan backup folder and update hash database for a channel.

    Returns number of files indexed.
    """
    if not backup_path:
        return 0

    backup_dir = Path(backup_path)
    if not backup_dir.exists():
        logger.warning(f"Backup path does not exist: {backup_path}")
        return 0

    # Get existing paths to skip
    with Database() as db:
        db.create_backup_hash_table(channel_id)
        existing_paths = db.get_existing_backup_paths(channel_id)
        existing_count = len(existing_paths)

    logger.info(f"    Backup path: {backup_path}")
    logger.info(f"    Already indexed: {existing_count} files")

    # Scan for new files
    new_hashes = []
    scanned_count = 0
    large_files = 0
    small_files = 0

    for subfolder in BACKUP_SUBFOLDERS:
        folder_path = backup_dir / subfolder
        if not folder_path.exists():
            logger.debug(f"    Subfolder not found: {subfolder}")
            continue

        logger.info(f"    Scanning {subfolder}/...")
        subfolder_count = 0

        for file_path in folder_path.rglob("*"):
            if not file_path.is_file():
                continue

            scanned_count += 1
            path_str = str(file_path)
            if path_str in existing_paths:
                continue  # Already indexed

            try:
                file_size = file_path.stat().st_size
                file_hash = compute_file_hash(file_path)
                new_hashes.append((path_str, file_size, file_hash))
                subfolder_count += 1
                if file_hash:
                    large_files += 1
                else:
                    small_files += 1
            except Exception as e:
                logger.warning(f"Error hashing {file_path}: {e}")

        if subfolder_count > 0:
            logger.info(f"      Found {subfolder_count} new files in {subfolder}/")

    # Batch insert new hashes
    if new_hashes:
        with Database() as db:
            db.insert_backup_hashes_batch(channel_id, new_hashes)
            db.commit()
        logger.info(f"    Indexed {len(new_hashes)} new backup files ({large_files} large with hash, {small_files} small)")
    else:
        logger.info(f"    No new files to index (scanned {scanned_count} files)")

    return len(new_hashes)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Concurrency settings for media downloads
CONCURRENT_DOWNLOADS = 5
MESSAGES_PER_BATCH = 500  # Download more messages per batch for history


async def sync_history_via_daemon() -> None:
    """Sync history using the TG daemon."""
    logger.info("Starting history sync via daemon...")

    # Run migrations
    DatabaseMigration().migrate()

    async with TGClient() as client:
        # Ping to verify connection
        status = await client.ping()
        logger.info(f"Connected to daemon (clients: {status['clients']})")

        # Get channels with download_all enabled
        with Database() as db:
            channels = [dict(row) for row in db.get_download_all_channels()]

        if not channels:
            logger.info("No channels with download_all enabled")
            return

        logger.info(f"Found {len(channels)} channels with download_all enabled")

        download_semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
        total_messages = 0
        total_media = 0
        total_from_backup = 0

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]
            backup_path = channel.get("backup_path")  # Optional backup location

            # Index backup folder hashes if backup_path is set
            if backup_path:
                logger.info(f"  {channel_title}: Scanning backup folder for new files...")
                update_backup_hashes(channel_id, backup_path)

            with Database() as db:
                oldest_id = db.get_oldest_message_id(channel_id)

            if oldest_id is None or oldest_id <= 1:
                logger.info(f"  {channel_title}: Already at beginning or no messages")
                continue

            backup_info = f" (backup: {backup_path})" if backup_path else ""
            logger.info(f"  {channel_title}: Downloading older messages (before id={oldest_id}){backup_info}...")

            try:
                # Fetch messages via daemon (skip polls)
                fetch_limit = MESSAGES_PER_BATCH * 2
                messages = await client.iter_messages(
                    channel_id, access_hash,
                    max_id=oldest_id,
                    limit=fetch_limit,
                )

                # Filter out polls
                raw_messages = [m for m in messages if m.get("media_type") != "poll"]
                raw_messages = raw_messages[:MESSAGES_PER_BATCH]

                if not raw_messages:
                    logger.info(f"    Reached beginning of channel history")
                    continue

                # Log message range
                newest_id = raw_messages[0]["id"] if raw_messages else None
                oldest_fetched_id = raw_messages[-1]["id"] if raw_messages else None
                logger.info(f"    Fetched {len(raw_messages)} messages (ids {oldest_fetched_id} - {newest_id})")

                # Identify messages with downloadable media
                downloadable_types = {"photo", "video", "audio", "voice", "document", "sticker", "animation"}
                messages_with_media = [
                    (msg, idx) for idx, msg in enumerate(raw_messages)
                    if msg.get("media_type") in downloadable_types and not msg.get("media_path")
                ]

                # Log media type breakdown
                media_type_counts: dict[str, int] = {}
                for msg in raw_messages:
                    mt = msg.get("media_type") or "none"
                    media_type_counts[mt] = media_type_counts.get(mt, 0) + 1
                type_breakdown = ", ".join(f"{k}:{v}" for k, v in sorted(media_type_counts.items()))
                logger.info(f"    Media types: {type_breakdown}")

                # Download media concurrently
                media_paths: dict[int, str | None] = {}
                from_backup_count = 0
                if messages_with_media:
                    logger.info(f"    Downloading {len(messages_with_media)} media files ({CONCURRENT_DOWNLOADS} concurrent)...")

                    async def download_one(msg: dict, idx: int) -> tuple[int, str | None, bool]:
                        async with download_semaphore:
                            msg_id = msg["id"]
                            media_type = msg.get("media_type", "unknown")
                            result = await client.download_media(
                                channel_id, access_hash, msg_id,
                                str(MEDIA_DIR),
                                backup_path=backup_path
                            )
                            path = result.get("path")
                            from_backup = result.get("from_backup", False)
                            if path:
                                if from_backup:
                                    logger.info(f"    [msg {msg_id}] {media_type}: copied from backup -> {path}")
                                else:
                                    logger.info(f"    [msg {msg_id}] {media_type}: downloaded from Telegram -> {path}")
                            elif result.get("error"):
                                logger.warning(f"    [msg {msg_id}] {media_type}: download failed - {result['error']}")
                            return (idx, path, from_backup)

                    tasks = [download_one(msg, idx) for msg, idx in messages_with_media]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"    Media download error: {result}")
                            continue
                        idx, path, from_backup = result
                        media_paths[idx] = path
                        if from_backup:
                            from_backup_count += 1

                # Build message dicts for database
                collected_messages = []
                media_count = 0
                for idx, msg in enumerate(raw_messages):
                    media_path = media_paths.get(idx, msg.get("media_path"))
                    if media_path:
                        media_count += 1

                    data = {
                        "id": msg["id"],
                        "date": msg.get("date"),
                        "message": msg.get("message"),
                        "entities": json.dumps(msg["entities"], ensure_ascii=False) if msg.get("entities") else None,
                        "media_type": msg.get("media_type"),
                        "media_path": media_path,
                        "grouped_id": msg.get("grouped_id"),
                        "reply_to_msg_id": msg.get("reply_to_msg_id"),
                        "forward_from": msg.get("forward_from"),
                        "read": 1,  # Mark historical messages as read
                    }
                    collected_messages.append(data)

                # Batch insert to DB
                message_count = len(collected_messages)
                if collected_messages:
                    with Database() as db:
                        db.insert_messages_batch(channel_id, collected_messages)
                        db.commit()

                total_messages += message_count
                total_media += media_count
                total_from_backup += from_backup_count
                backup_str = f" ({from_backup_count} from backup)" if from_backup_count else ""
                logger.info(f"    Downloaded {message_count} older messages, {media_count} media files{backup_str}")

            except TGFloodWaitError as e:
                logger.warning(f"    FloodWait: must wait {e.seconds} seconds, skipping to next channel")
                continue
            except Exception as e:
                logger.error(f"    Error downloading older messages: {e}")
                continue

        logger.info("=" * 50)
        backup_str = f" ({total_from_backup} from backup)" if total_from_backup else ""
        logger.info(f"History sync completed! {total_messages} messages, {total_media} media files{backup_str}")
        logger.info("=" * 50)


async def sync_history_direct() -> None:
    """Sync history using direct Telethon connection (fallback)."""
    import json
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
    from telethon.tl.types import (
        Message,
        MessageMediaPhoto,
        MessageMediaDocument,
        MessageMediaWebPage,
        MessageMediaPoll,
        InputChannel,
    )
    from config import API_ID, API_HASH, PHONE_NUMBER, SESSION_PATH, validate_config

    def is_poll_message(msg: Message) -> bool:
        """Check if a message is a poll (should be skipped)."""
        return isinstance(msg.media, MessageMediaPoll)

    def get_media_type(media) -> str | None:
        """Get a string representation of the media type."""
        if media is None:
            return None
        if isinstance(media, MessageMediaPhoto):
            return "photo"
        if isinstance(media, MessageMediaDocument):
            doc = media.document
            if doc:
                for attr in doc.attributes:
                    attr_name = type(attr).__name__
                    if attr_name == "DocumentAttributeVideo":
                        return "video"
                    if attr_name == "DocumentAttributeAudio":
                        if getattr(attr, "voice", False):
                            return "voice"
                        return "audio"
                    if attr_name == "DocumentAttributeSticker":
                        return "sticker"
                    if attr_name == "DocumentAttributeAnimated":
                        return "animation"
                return "document"
            return "document"
        if isinstance(media, MessageMediaWebPage):
            return "webpage"
        if isinstance(media, MessageMediaPoll):
            return "poll"
        return "unknown"

    def has_downloadable_media(msg: Message) -> bool:
        """Check if a message has media that should be downloaded."""
        if not msg.media:
            return False
        media_type = get_media_type(msg.media)
        return media_type in ("photo", "video", "audio", "voice", "document", "sticker", "animation")

    def extract_entities(msg: Message) -> str | None:
        """Extract message entities as JSON."""
        if not msg.entities:
            return None
        entities = []
        for entity in msg.entities:
            entity_data = {
                "offset": entity.offset,
                "length": entity.length,
                "type": type(entity).__name__,
            }
            if hasattr(entity, 'url'):
                entity_data["url"] = entity.url
            if hasattr(entity, 'language'):
                entity_data["language"] = entity.language
            entities.append(entity_data)
        return json.dumps(entities, ensure_ascii=False) if entities else None

    def message_to_dict(msg: Message, media_path: str | None = None) -> dict:
        """Convert a Telegram message to a dictionary for database storage."""
        return {
            "id": msg.id,
            "date": int(msg.date.timestamp()) if msg.date else None,
            "message": msg.message,
            "entities": extract_entities(msg),
            "media_type": get_media_type(msg.media),
            "media_path": media_path,
            "grouped_id": msg.grouped_id,
            "reply_to": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
            "forward_from": None,
        }

    def get_expected_media_filename(msg: Message) -> str | None:
        """Get the expected filename that Telethon would use for this message's media."""
        if not msg.media:
            return None

        if isinstance(msg.media, MessageMediaPhoto):
            if msg.media.photo and msg.date:
                date_str = msg.date.strftime("%Y-%m-%d_%H-%M-%S")
                return f"photo_{date_str}.jpg"
            return None

        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if not doc:
                return None
            for attr in doc.attributes:
                if type(attr).__name__ == "DocumentAttributeFilename":
                    return attr.file_name
            return None

        return None

    def get_media_file_size(msg: Message) -> int | None:
        """Get the file size of media in a message."""
        if not msg.media:
            return None

        if isinstance(msg.media, MessageMediaPhoto):
            if msg.media.photo and msg.media.photo.sizes:
                for size in reversed(msg.media.photo.sizes):
                    if hasattr(size, "size"):
                        return size.size
            return None

        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc and hasattr(doc, "size"):
                return doc.size
            return None

        return None

    def find_file_in_backup_by_hash(channel_id: int, msg: Message,
                                     downloaded_path: Path) -> str | None:
        """Find a media file in backup using content hash matching.

        Only used for large files (>64KB). Small files are downloaded directly.
        """
        if not downloaded_path or not downloaded_path.exists():
            logger.debug(f"      Hash check skipped: file doesn't exist")
            return None

        actual_size = downloaded_path.stat().st_size

        # Compute hash of the downloaded file
        file_hash = compute_file_hash(downloaded_path)
        if not file_hash:
            logger.warning(f"      Could not compute hash for {downloaded_path}")
            return None

        with Database() as db:
            backup_count = db.get_backup_hash_count(channel_id)
            logger.info(f"      File size: {actual_size:,} bytes, hash: {file_hash}, checking {backup_count} indexed backups...")
            match = db.find_backup_by_hash(channel_id, file_hash)
            if match and Path(match).exists():
                logger.info(f"      Hash match found: {match}")
                return match
            elif match:
                logger.warning(f"      Hash matched but file missing: {match}")
            else:
                logger.info(f"      No backup match for hash {file_hash}")

        return None

    def find_file_in_backup(backup_path: str, expected_filename: str | None, msg: Message) -> str | None:
        """Search for a media file in the backup path (legacy filename-based)."""
        backup_dir = Path(backup_path)
        if not backup_dir.exists():
            return None

        # Strategy 1: Exact filename match (recursive)
        if expected_filename:
            for path in backup_dir.rglob(expected_filename):
                return str(path)

        # Strategy 2: For photos, try matching by date pattern
        if isinstance(msg.media, MessageMediaPhoto) and msg.date:
            date_str = msg.date.strftime("%Y-%m-%d_%H-%M-%S")
            for path in backup_dir.rglob(f"photo_{date_str}*.jpg"):
                return str(path)
            date_str2 = msg.date.strftime("%Y%m%d_%H%M%S")
            for path in backup_dir.rglob(f"*{date_str2}*.jpg"):
                return str(path)

        # Strategy 3: For documents with known filename, try partial match
        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc:
                for attr in doc.attributes:
                    if type(attr).__name__ == "DocumentAttributeFilename":
                        filename = attr.file_name
                        for path in backup_dir.rglob(filename):
                            return str(path)
                        base_name = Path(filename).stem
                        ext = Path(filename).suffix
                        for path in backup_dir.rglob(f"*{base_name}*{ext}"):
                            return str(path)

        return None

    async def download_media(client: TelegramClient, msg: Message, channel_id: int,
                            backup_path: str | None = None) -> tuple[str | None, bool]:
        """Download media from a message and return (relative_path, from_backup)."""
        if not msg.media:
            return None, False

        media_type = get_media_type(msg.media)
        if media_type not in ("photo", "video", "audio", "voice", "document", "sticker", "animation"):
            return None, False

        channel_dir = MEDIA_DIR / str(channel_id)
        channel_dir.mkdir(parents=True, exist_ok=True)

        # Get expected size from Telegram metadata
        expected_size = get_media_file_size(msg)
        size_str = f"{expected_size:,} bytes" if expected_size else "unknown size"

        # Download from Telegram
        try:
            path = await client.download_media(msg, file=channel_dir)
            if path:
                downloaded_path = Path(path)
                actual_size = downloaded_path.stat().st_size

                # For large files (>64KB), check hash after download
                # Small files are just downloaded directly - no matching needed
                if backup_path:
                    if expected_size is not None and expected_size > HASH_SIZE_THRESHOLD:
                        logger.info(f"      Large file ({actual_size:,} bytes), checking backup hash...")
                        backup_file = find_file_in_backup_by_hash(channel_id, msg, downloaded_path)
                        if backup_file:
                            import shutil
                            src_path = Path(backup_file)
                            dest_file = channel_dir / src_path.name
                            try:
                                # Remove downloaded file and use backup
                                downloaded_path.unlink()
                                if not dest_file.exists():
                                    shutil.copy2(backup_file, dest_file)
                                logger.info(f"      Replaced with backup (hash match): {backup_file}")
                                rel_path = f"{channel_id}/{src_path.name}"
                                return rel_path, True
                            except Exception as e:
                                logger.warning(f"      Failed to use backup after hash match: {e}")
                    else:
                        logger.debug(f"      Small file ({actual_size:,} bytes <= {HASH_SIZE_THRESHOLD:,}), no backup check needed")
                else:
                    logger.debug(f"      No backup path configured, skipping backup check")

                rel_path = f"{channel_id}/{downloaded_path.name}"
                return rel_path, False
        except Exception as e:
            logger.error(f"    Failed to download media for message {msg.id}: {e}")

        return None, False

    async def download_media_concurrent(
        client: TelegramClient,
        messages_with_media: list[tuple[Message, int]],
        channel_id: int,
        semaphore: asyncio.Semaphore,
        backup_path: str | None = None,
    ) -> tuple[dict[int, str | None], int]:
        """Download media for multiple messages concurrently.
        Returns (media_paths, from_backup_count)."""
        from_backup_count = 0

        async def download_with_semaphore(msg: Message, idx: int) -> tuple[int, str | None, bool]:
            async with semaphore:
                msg_id = msg.id
                media_type = get_media_type(msg.media) or "unknown"
                path, from_backup = await download_media(client, msg, channel_id, backup_path)
                if path:
                    if from_backup:
                        logger.info(f"    [msg {msg_id}] {media_type}: copied from backup -> {path}")
                    else:
                        logger.info(f"    [msg {msg_id}] {media_type}: downloaded from Telegram -> {path}")
                return (idx, path, from_backup)

        tasks = [download_with_semaphore(msg, idx) for msg, idx in messages_with_media]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        media_paths: dict[int, str | None] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"    Media download error: {result}")
                continue
            idx, path, from_backup = result
            media_paths[idx] = path
            if from_backup:
                from_backup_count += 1

        return media_paths, from_backup_count

    logger.info("Starting history sync (direct connection)...")

    validate_config()

    # Run migrations
    DatabaseMigration().migrate()

    # Get channels with download_all enabled
    with Database() as db:
        channels = [dict(row) for row in db.get_download_all_channels()]

    if not channels:
        logger.info("No channels with download_all enabled")
        return

    # Show backup configuration summary
    channels_with_backup = [c for c in channels if c.get("backup_path")]
    if channels_with_backup:
        logger.info(f"Found {len(channels)} channels with download_all enabled ({len(channels_with_backup)} have backup configured)")
        for c in channels_with_backup:
            logger.info(f"  - {c['title']}: backup_path = {c['backup_path']}")
    else:
        logger.info(f"Found {len(channels)} channels with download_all enabled (none have backup configured)")

    # Connect to Telegram
    logger.info("Connecting to Telegram...")
    client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH)

    try:
        await client.start(phone=PHONE_NUMBER)
        logger.info("Connected to Telegram")

        download_semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
        total_messages = 0
        total_media = 0
        total_from_backup = 0

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]
            backup_path = channel.get("backup_path")  # Optional backup location

            # Index backup folder hashes if backup_path is set
            if backup_path:
                logger.info(f"  {channel_title}: Scanning backup folder for new files...")
                update_backup_hashes(channel_id, backup_path)

            with Database() as db:
                oldest_id = db.get_oldest_message_id(channel_id)

            if oldest_id is None or oldest_id <= 1:
                logger.info(f"  {channel_title}: Already at beginning or no messages")
                continue

            backup_info = f" (backup: {backup_path})" if backup_path else ""
            logger.info(f"  {channel_title}: Downloading older messages (before id={oldest_id}){backup_info}...")

            try:
                input_channel = InputChannel(channel_id, access_hash)

                # Phase 1: Collect messages (skip polls)
                raw_messages: list[Message] = []
                fetch_limit = MESSAGES_PER_BATCH * 2

                async for msg in client.iter_messages(
                    input_channel,
                    max_id=oldest_id,
                    limit=fetch_limit,
                ):
                    if isinstance(msg, Message) and not is_poll_message(msg):
                        raw_messages.append(msg)
                        if len(raw_messages) >= MESSAGES_PER_BATCH:
                            break

                if not raw_messages:
                    logger.info(f"    Reached beginning of channel history")
                    continue

                # Log message range
                newest_id = raw_messages[0].id if raw_messages else None
                oldest_fetched_id = raw_messages[-1].id if raw_messages else None
                logger.info(f"    Fetched {len(raw_messages)} messages (ids {oldest_fetched_id} - {newest_id})")

                # Phase 2: Identify messages with downloadable media
                messages_with_media: list[tuple[Message, int]] = []
                for idx, msg in enumerate(raw_messages):
                    if has_downloadable_media(msg):
                        messages_with_media.append((msg, idx))

                # Log media type breakdown
                media_type_counts: dict[str, int] = {}
                for msg in raw_messages:
                    mt = get_media_type(msg.media) or "none"
                    media_type_counts[mt] = media_type_counts.get(mt, 0) + 1
                type_breakdown = ", ".join(f"{k}:{v}" for k, v in sorted(media_type_counts.items()))
                logger.info(f"    Media types: {type_breakdown}")

                # Phase 3: Download all media concurrently (with backup support)
                media_paths: dict[int, str | None] = {}
                from_backup_count = 0
                if messages_with_media:
                    if backup_path:
                        with Database() as db:
                            backup_count = db.get_backup_hash_count(channel_id)
                        logger.info(f"    Downloading {len(messages_with_media)} media files ({CONCURRENT_DOWNLOADS} concurrent), backup enabled with {backup_count} indexed files")
                    else:
                        logger.info(f"    Downloading {len(messages_with_media)} media files ({CONCURRENT_DOWNLOADS} concurrent), no backup configured")
                    media_paths, from_backup_count = await download_media_concurrent(
                        client, messages_with_media, channel_id, download_semaphore, backup_path
                    )

                # Phase 4: Build message dicts with media paths
                collected_messages = []
                media_count = 0
                for idx, msg in enumerate(raw_messages):
                    media_path = media_paths.get(idx)
                    if media_path:
                        media_count += 1

                    data = message_to_dict(msg, media_path)
                    data["read"] = 1  # Mark historical messages as read
                    collected_messages.append(data)

                # Phase 5: Batch insert to DB
                message_count = len(collected_messages)
                if collected_messages:
                    with Database() as db:
                        db.insert_messages_batch(channel_id, collected_messages)
                        db.commit()

                total_messages += message_count
                total_media += media_count
                total_from_backup += from_backup_count
                backup_str = f" ({from_backup_count} from backup)" if from_backup_count else ""
                logger.info(f"    Downloaded {message_count} older messages, {media_count} media files{backup_str}")

            except FloodWaitError as e:
                logger.warning(f"    FloodWait: must wait {e.seconds} seconds, skipping to next channel")
                continue
            except Exception as e:
                logger.error(f"    Error downloading older messages: {e}")
                continue

        logger.info("=" * 50)
        backup_str = f" ({total_from_backup} from backup)" if total_from_backup else ""
        logger.info(f"History sync completed! {total_messages} messages, {total_media} media files{backup_str}")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await client.disconnect()
        logger.info("Disconnected from Telegram")


async def sync_history() -> None:
    """Sync history - uses daemon if available, else direct connection."""
    if await is_daemon_running():
        logger.info("TG daemon is running, using RPC")
        try:
            await sync_history_via_daemon()
            return
        except TGClientConnectionError as e:
            logger.warning(f"Daemon connection failed: {e}")
            logger.info("Falling back to direct connection...")

    await sync_history_direct()


if __name__ == "__main__":
    asyncio.run(sync_history())
