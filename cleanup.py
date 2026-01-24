"""Clean up old messages from channels not marked for full history download."""

import logging
import os
import sys
import time
from pathlib import Path

from config import MEDIA_DIR, validate_config
from database import Database

# Configure logging with UTF-8 support for Windows
import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Messages older than this will be deleted (30 days)
MAX_AGE_SECONDS = 30 * 24 * 60 * 60


def cleanup_old_messages() -> None:
    """Delete old messages and media from channels without download_all enabled."""
    logger.info("Starting cleanup...")

    validate_config()

    cutoff_date = int(time.time()) - MAX_AGE_SECONDS
    logger.info(f"Deleting messages older than {MAX_AGE_SECONDS // (24 * 60 * 60)} days")

    # Get channels that are NOT marked for full history download
    with Database() as db:
        cursor = db.conn.cursor()
        cursor.execute("""
            SELECT id, title FROM channels
            WHERE (download_all = 0 OR download_all IS NULL)
            AND subscribed = 1
        """)
        channels = cursor.fetchall()

    if not channels:
        logger.info("No channels to clean up")
        return

    logger.info(f"Found {len(channels)} channels to clean up")

    total_messages_deleted = 0
    total_files_deleted = 0
    total_bytes_freed = 0

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]
        table_name = f"channel_{channel_id}"

        try:
            with Database() as db:
                cursor = db.conn.cursor()

                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    # Table doesn't exist - channel has no messages yet
                    continue

                # First, get IDs and media paths for messages we're about to delete
                cursor.execute(f"""
                    SELECT id, media_path FROM {table_name}
                    WHERE date < ?
                """, (cutoff_date,))
                old_messages = cursor.fetchall()

                if not old_messages:
                    continue

                # Collect message IDs for FTS cleanup
                message_ids = [row["id"] for row in old_messages]

                # Delete the media files
                files_deleted = 0
                bytes_freed = 0
                for row in old_messages:
                    media_path = row["media_path"]
                    if media_path:
                        full_path = MEDIA_DIR / media_path
                        if full_path.exists():
                            try:
                                file_size = full_path.stat().st_size
                                full_path.unlink()
                                files_deleted += 1
                                bytes_freed += file_size
                            except OSError as e:
                                logger.warning(f"  Failed to delete {full_path}: {e}")

                # Delete from FTS index
                placeholders = ",".join("?" * len(message_ids))
                cursor.execute(f"""
                    DELETE FROM messages_fts
                    WHERE channel_id = ? AND message_id IN ({placeholders})
                """, [channel_id] + message_ids)
                fts_deleted = cursor.rowcount

                # Delete old messages from database
                cursor.execute(f"""
                    DELETE FROM {table_name} WHERE date < ?
                """, (cutoff_date,))
                messages_deleted = cursor.rowcount

                db.commit()

                if messages_deleted > 0 or files_deleted > 0:
                    fts_info = f", {fts_deleted} FTS entries" if fts_deleted > 0 else ""
                    logger.info(
                        f"  {channel_title}: deleted {messages_deleted} messages, "
                        f"{files_deleted} files ({bytes_freed / 1024 / 1024:.1f} MB){fts_info}"
                    )
                    total_messages_deleted += messages_deleted
                    total_files_deleted += files_deleted
                    total_bytes_freed += bytes_freed

        except Exception as e:
            logger.error(f"  Error cleaning up {channel_title}: {e}")
            continue

    # Clean up empty media directories
    logger.info("Cleaning up empty directories...")
    for channel_dir in MEDIA_DIR.iterdir():
        if channel_dir.is_dir():
            try:
                # Only remove if empty
                if not any(channel_dir.iterdir()):
                    channel_dir.rmdir()
                    logger.info(f"  Removed empty directory: {channel_dir.name}")
            except OSError:
                pass

    logger.info("=" * 50)
    logger.info(
        f"Cleanup completed: {total_messages_deleted} messages, "
        f"{total_files_deleted} files ({total_bytes_freed / 1024 / 1024:.1f} MB freed)"
    )
    logger.info("=" * 50)


if __name__ == "__main__":
    cleanup_old_messages()
