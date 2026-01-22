"""Sync read messages from TGFeed to Telegram.

Uses tg_daemon if available, falls back to direct Telethon connection.
"""

import asyncio
import logging
import sys

from database import Database, DatabaseMigration
from tg_client import TGClient, TGClientConnectionError, TGFloodWaitError, is_daemon_running

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def sync_read_via_daemon() -> None:
    """Sync read status using the TG daemon."""
    logger.info("Starting read status sync to Telegram via daemon...")

    # Run migrations
    DatabaseMigration().migrate()

    async with TGClient() as client:
        # Ping to verify connection
        status = await client.ping()
        logger.info(f"Connected to daemon (clients: {status['clients']})")

        # Get active channels
        with Database() as db:
            channels = [dict(row) for row in db.get_active_channels()]

        if not channels:
            logger.warning("No active channels found.")
            return

        logger.info(f"Found {len(channels)} active channels")

        total_synced = 0

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]

            # Get messages that are read locally but not synced to Telegram
            with Database() as db:
                messages = db.get_unsynced_read_messages(channel_id, limit=1000)

            if not messages:
                continue

            # Find the highest message ID to mark as read
            max_id = max(m["id"] for m in messages)

            logger.info(f"  {channel_title}: {len(messages)} messages to sync (up to id={max_id})")

            try:
                result = await client.send_read_acknowledge(channel_id, access_hash, max_id)

                if result.get("success"):
                    # Mark as synced in database
                    with Database() as db:
                        updated = db.mark_messages_synced_to_tg(channel_id, max_id)
                        db.commit()
                        total_synced += updated
                        logger.info(f"  {channel_title}: Synced {updated} messages as read in Telegram")
                else:
                    logger.error(f"  {channel_title}: Failed to sync read status")

            except TGFloodWaitError as e:
                logger.warning(f"  {channel_title}: FloodWait - must wait {e.seconds} seconds, skipping")
                continue
            except Exception as e:
                logger.error(f"  {channel_title}: Failed to sync read status: {e}")

        logger.info("=" * 50)
        logger.info(f"Read status sync completed! {total_synced} messages synced to Telegram")
        logger.info("=" * 50)


async def sync_read_direct() -> None:
    """Sync read status using direct Telethon connection (fallback)."""
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
    from telethon.tl.types import InputChannel
    from config import API_ID, API_HASH, PHONE_NUMBER, SESSION_PATH, validate_config

    logger.info("Starting read status sync to Telegram (direct connection)...")

    validate_config()

    # Run migrations
    DatabaseMigration().migrate()

    # Get active channels
    with Database() as db:
        channels = [dict(row) for row in db.get_active_channels()]

    if not channels:
        logger.warning("No active channels found.")
        return

    logger.info(f"Found {len(channels)} active channels")

    # Connect to Telegram
    logger.info("Connecting to Telegram...")
    client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH)

    try:
        await client.start(phone=PHONE_NUMBER)
        logger.info("Connected to Telegram")

        total_synced = 0

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]

            # Get messages that are read locally but not synced to Telegram
            with Database() as db:
                messages = db.get_unsynced_read_messages(channel_id, limit=1000)

            if not messages:
                continue

            # Find the highest message ID to mark as read
            max_id = max(m["id"] for m in messages)

            logger.info(f"  {channel_title}: {len(messages)} messages to sync (up to id={max_id})")

            try:
                input_channel = InputChannel(channel_id, access_hash)
                await client.send_read_acknowledge(input_channel, max_id=max_id)

                # Mark as synced in database
                with Database() as db:
                    updated = db.mark_messages_synced_to_tg(channel_id, max_id)
                    db.commit()
                    total_synced += updated
                    logger.info(f"  {channel_title}: Synced {updated} messages as read in Telegram")

            except FloodWaitError as e:
                logger.warning(f"  {channel_title}: FloodWait - must wait {e.seconds} seconds, skipping")
                continue
            except Exception as e:
                logger.error(f"  {channel_title}: Failed to sync read status: {e}")

        logger.info("=" * 50)
        logger.info(f"Read status sync completed! {total_synced} messages synced to Telegram")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await client.disconnect()
        logger.info("Disconnected from Telegram")


async def sync_read_to_telegram() -> None:
    """Sync read status - uses daemon if available, else direct connection."""
    if await is_daemon_running():
        logger.info("TG daemon is running, using RPC")
        try:
            await sync_read_via_daemon()
            return
        except TGClientConnectionError as e:
            logger.warning(f"Daemon connection failed: {e}")
            logger.info("Falling back to direct connection...")

    await sync_read_direct()


if __name__ == "__main__":
    asyncio.run(sync_read_to_telegram())
