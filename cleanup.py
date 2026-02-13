"""Clean up read messages and media from channels not marked for full history download."""

import logging
import os
import sys
import time
from pathlib import Path

from config import MEDIA_DIR, validate_config
from database import Database

# Configure logging with UTF-8 support for Windows
if sys.platform == "win32":
    os.system('')  # Enable ANSI escape sequences
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Media files deleted 7 days after reading
MEDIA_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
# Messages deleted 30 days after reading
MESSAGE_MAX_AGE_SECONDS = 30 * 24 * 60 * 60


def cleanup_old_messages() -> None:
    """Delete read messages and media from channels without download_all enabled.

    Two-phase cleanup:
    1. Media files are deleted from disk 7 days after reading (media_path cleared in DB)
    2. Message rows are deleted from DB 30 days after reading (+ FTS cleanup)

    Uses read_at timestamp, falling back to created_at for old messages without read_at.
    Bookmarked messages are never cleaned up.
    """
    logger.info("Starting cleanup...")

    validate_config()

    now = int(time.time())
    media_cutoff = now - MEDIA_MAX_AGE_SECONDS
    message_cutoff = now - MESSAGE_MAX_AGE_SECONDS
    logger.info(
        f"Media: {MEDIA_MAX_AGE_SECONDS // (24 * 60 * 60)}d after read, "
        f"Messages: {MESSAGE_MAX_AGE_SECONDS // (24 * 60 * 60)}d after read"
    )

    # Get channels that are NOT marked for full history download
    with Database() as db:
        cursor = db.cursor()
        cursor.execute("""
            SELECT id, title FROM channels
            WHERE (download_all = 0 OR download_all IS NULL)
            AND active = 1
        """)
        channels = cursor.fetchall()

    if not channels:
        logger.info("No channels to clean up")
        return

    logger.info(f"Found {len(channels)} channels to clean up")

    total_messages_deleted = 0
    total_media_cleared = 0
    total_files_deleted = 0
    total_bytes_freed = 0

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]
        table_name = f"channel_{channel_id}"

        try:
            with Database() as db:
                cursor = db.cursor()

                # Check if table exists
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    continue

                # --- Phase 1: Delete media files (7 days after reading) ---
                cursor.execute(f"""
                    SELECT id, media_path FROM {table_name}
                    WHERE read = 1
                      AND media_path IS NOT NULL
                      AND COALESCE(read_at, created_at) < ?
                      AND (bookmarked = 0 OR bookmarked IS NULL)
                """, (media_cutoff,))
                media_messages = cursor.fetchall()

                files_deleted = 0
                bytes_freed = 0
                for row in media_messages:
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

                # Clear media_path in DB for deleted files
                if media_messages:
                    media_ids = [row["id"] for row in media_messages]
                    placeholders = ",".join("?" * len(media_ids))
                    cursor.execute(f"""
                        UPDATE {table_name}
                        SET media_path = NULL, video_thumbnail_path = NULL
                        WHERE id IN ({placeholders})
                    """, media_ids)
                    media_cleared = cursor.rowcount

                    total_media_cleared += media_cleared
                    total_files_deleted += files_deleted
                    total_bytes_freed += bytes_freed

                # --- Phase 2: Delete message rows (30 days after reading) ---
                # Keep the most recent message always
                cursor.execute(f"SELECT MAX(id) FROM {table_name}")
                max_id_row = cursor.fetchone()
                max_id = max_id_row[0] if max_id_row and max_id_row[0] else None
                if max_id is None:
                    db.commit()
                    continue

                cursor.execute(f"""
                    SELECT id, media_path FROM {table_name}
                    WHERE read = 1
                      AND COALESCE(read_at, created_at) < ?
                      AND id != ?
                      AND (bookmarked = 0 OR bookmarked IS NULL)
                """, (message_cutoff, max_id))
                old_messages = cursor.fetchall()

                if not old_messages:
                    db.commit()
                    if files_deleted > 0:
                        logger.info(
                            f"  {channel_title}: cleared {files_deleted} media files "
                            f"({bytes_freed / 1024 / 1024:.1f} MB)"
                        )
                    continue

                message_ids = [row["id"] for row in old_messages]

                # Delete any remaining media files (shouldn't be many after phase 1)
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
                                total_files_deleted += 1
                                total_bytes_freed += file_size
                            except OSError as e:
                                logger.warning(f"  Failed to delete {full_path}: {e}")

                # Delete from FTS index
                placeholders = ",".join("?" * len(message_ids))
                cursor.execute(f"""
                    DELETE FROM messages_fts
                    WHERE channel_id = ? AND message_id IN ({placeholders})
                """, [channel_id] + message_ids)
                fts_deleted = cursor.rowcount

                # Delete message rows
                cursor.execute(f"""
                    DELETE FROM {table_name}
                    WHERE read = 1
                      AND COALESCE(read_at, created_at) < ?
                      AND id != ?
                      AND (bookmarked = 0 OR bookmarked IS NULL)
                """, (message_cutoff, max_id))
                messages_deleted = cursor.rowcount

                db.commit()

                total_messages_deleted += messages_deleted

                if messages_deleted > 0 or files_deleted > 0:
                    fts_info = f", {fts_deleted} FTS entries" if fts_deleted > 0 else ""
                    logger.info(
                        f"  {channel_title}: deleted {messages_deleted} messages, "
                        f"{files_deleted} files ({bytes_freed / 1024 / 1024:.1f} MB){fts_info}"
                    )

        except Exception as e:
            logger.error(f"  Error cleaning up {channel_title}: {e}")
            continue

    # Clean up empty media directories
    logger.info("Cleaning up empty directories...")
    for channel_dir in MEDIA_DIR.iterdir():
        if channel_dir.is_dir():
            try:
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
