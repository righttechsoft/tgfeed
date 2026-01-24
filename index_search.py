#!/usr/bin/env python3
"""Index messages for full-text search.

This script indexes all messages into the FTS5 search index.
It checks what's already indexed and only adds new messages.

Usage:
    uv run python index_search.py [--optimize] [--rebuild]

Options:
    --optimize  Run FTS5 optimize after indexing (merges b-trees)
    --rebuild   Clear and rebuild the entire search index
"""

import argparse
import logging
import sys

from database import Database, DatabaseMigration

# Configure logging with UTF-8 support for Windows
import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Batch size for indexing
BATCH_SIZE = 500


def index_channel_messages(db: Database, channel_id: int, channel_title: str,
                           rebuild: bool = False) -> int:
    """Index messages for a single channel.

    Args:
        db: Database connection
        channel_id: Channel ID to index
        channel_title: Channel title for logging
        rebuild: If True, index all messages. If False, only index new ones.

    Returns:
        Number of messages indexed.
    """
    # Get all messages that can be indexed
    all_messages = db.get_all_messages_for_indexing(channel_id)
    if not all_messages:
        return 0

    if rebuild:
        # Index everything
        to_index = all_messages
    else:
        # Get what's already indexed
        indexed_ids = db.get_indexed_message_ids(channel_id)
        # Find messages not yet indexed
        to_index = [m for m in all_messages if m["id"] not in indexed_ids]

    if not to_index:
        return 0

    # Index in batches
    total_indexed = 0
    for i in range(0, len(to_index), BATCH_SIZE):
        batch = to_index[i:i + BATCH_SIZE]
        indexed = db.index_messages_batch(channel_id, batch)
        total_indexed += indexed
        db.commit()

    if total_indexed > 0:
        logger.info(f"  {channel_title}: indexed {total_indexed} messages")

    return total_indexed


def main():
    parser = argparse.ArgumentParser(description="Index messages for full-text search")
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run FTS5 optimize after indexing"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Clear and rebuild the entire search index"
    )
    args = parser.parse_args()

    # Run migrations first to ensure FTS table exists
    migration = DatabaseMigration()
    migration.migrate()

    logger.info("Starting search index update...")

    total_indexed = 0

    with Database() as db:
        # Handle rebuild
        if args.rebuild:
            logger.info("Rebuilding search index from scratch...")
            db.clear_search_index()
            db.commit()

        # Get index stats before
        stats_before = db.get_search_index_stats()
        logger.info(f"Current index size: {stats_before['indexed_messages']} messages")

        # Get all active channels
        channels = db.get_active_channels()
        logger.info(f"Processing {len(channels)} active channels")

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]

            indexed = index_channel_messages(db, channel_id, channel_title, args.rebuild)
            total_indexed += indexed

        # Get index stats after
        stats_after = db.get_search_index_stats()

        if total_indexed > 0:
            logger.info(f"Indexed {total_indexed} new messages")
            logger.info(f"Index size: {stats_before['indexed_messages']} -> {stats_after['indexed_messages']}")

        # Optimize if requested
        if args.optimize:
            logger.info("Optimizing search index...")
            db.optimize_search_index()
            db.commit()

    if total_indexed == 0:
        logger.info("No new messages to index")

    return 0


if __name__ == "__main__":
    sys.exit(main())
