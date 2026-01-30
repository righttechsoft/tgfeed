"""Generate semantic content hashes for message deduplication using Claude API.

Processes messages to create normalized summaries, then hashes them for
cross-channel duplicate detection.
"""

import hashlib
import logging
import sys
import time

import requests

from config import (
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
    DEDUP_MESSAGES_PER_RUN,
    DEDUP_MIN_MESSAGE_LENGTH,
    validate_config,
)
from database import Database, DatabaseMigration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Rate limiting
API_DELAY_SECONDS = 1.0  # Delay between API calls


# System prompt for Claude
SYSTEM_PROMPT = """Create a short headline for this post in a short sentence. Focus on:
- What is the core topic or news?
- What are the key facts, names, numbers, or events?
- Ignore greetings, calls to action, formatting, and promotional language.

ALWAYS USE STRUCTURE: Who, When, What, How.

Output ONLY the summary, nothing else. Maximum 100 words. Always use English, regardless of the post language!

If the post is clearly an advertising, respond with just a single word `advertising`"""


def call_claude_api(message_text: str) -> str | None:
    """Call Claude API to generate a summary of the message.

    Returns the summary, or None on error.
    """
    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not configured")
        return None

    url = "https://api.anthropic.com/v1/messages"

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json"
    }

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 150,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": message_text}],
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result["content"][0]["text"].strip()
    except requests.exceptions.RequestException as e:
        logger.error(f"Claude API error: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected API response format: {e}")
        return None


def compute_hash(normalized_text: str) -> str:
    """Generate SHA256 hash from normalized text."""
    # Lowercase and strip for consistency
    clean_text = normalized_text.strip().lower()
    return hashlib.sha256(clean_text.encode('utf-8')).hexdigest()


def generate_content_hashes() -> None:
    """Main function to generate content hashes for deduplication."""
    logger.info("Starting content hash generation...")

    validate_config()

    if not ANTHROPIC_API_KEY:
        logger.error("ANTHROPIC_API_KEY not set in .env - cannot proceed")
        sys.exit(1)

    # Run migrations to ensure columns exist
    DatabaseMigration().migrate()

    # Get active channels
    with Database() as db:
        channels = [dict(row) for row in db.get_active_channels()]

    if not channels:
        logger.info("No active channels found")
        return

    logger.info(f"Found {len(channels)} active channels")

    total_processed = 0
    total_duplicates = 0
    total_skipped = 0
    total_errors = 0

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]

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
            message_text = msg["message"]

            # Rate limit
            time.sleep(API_DELAY_SECONDS)

            # Generate AI summary via Claude
            ai_summary = call_claude_api(message_text)

            if not ai_summary:
                logger.warning(f"  Failed to get summary for message {msg['id']}")
                total_errors += 1
                continue

            # Compute hash from the AI summary
            content_hash = compute_hash(ai_summary)

            # Check for duplicates and register
            with Database() as db:
                original = db.register_content_hash(
                    content_hash, channel_id, msg["id"], msg["date"]
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

    logger.info("=" * 50)
    logger.info("Content hash generation completed!")
    logger.info(f"  Processed: {total_processed}")
    logger.info(f"  Duplicates found: {total_duplicates}")
    logger.info(f"  Skipped (short): {total_skipped}")
    logger.info(f"  Errors: {total_errors}")
    logger.info("=" * 50)


if __name__ == "__main__":
    return
    #generate_content_hashes()
