"""Sync channels from Telegram to local database.

Uses tg_daemon if available, falls back to direct Telethon connection.
"""

import asyncio
import logging
import sys
import time

from config import DATA_DIR
from database import Database, DatabaseMigration
from tg_client import TGClient, TGClientConnectionError, TGFloodWaitError, is_daemon_running

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


async def sync_channels_via_daemon() -> None:
    """Sync channels using the TG daemon."""
    logger.info("Starting channel sync via daemon...")

    # Run migrations
    DatabaseMigration().migrate()

    async with TGClient() as client:
        # Ping to verify connection
        status = await client.ping()
        logger.info(f"Connected to daemon (clients: {status['clients']})")

        # Fetch broadcast channels (with flood wait handling)
        logger.info("Fetching channels...")
        try:
            dialogs = await client.iter_dialogs()
        except TGFloodWaitError as e:
            logger.warning(f"FloodWait on iter_dialogs: waiting {e.seconds} seconds...")
            await asyncio.sleep(e.seconds + 1)
            dialogs = await client.iter_dialogs()

        logger.info(f"Found {len(dialogs)} broadcast channels")

        # Download channel photos
        photos_dir = DATA_DIR / "photos"
        photos_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading channel photos...")

        for ch in dialogs:
            photo_path = photos_dir / f"{ch['id']}.jpg"
            if not photo_path.exists():
                try:
                    result = await client.download_profile_photo(
                        ch["id"], ch["access_hash"], str(photo_path)
                    )
                    if result.get("path"):
                        logger.info(f"Downloaded photo for {ch['title']}")
                except Exception as e:
                    logger.warning(f"Could not download photo for {ch['title']}: {e}")

        # Merge to database
        now = int(time.time())
        with Database() as db:
            existing_ids = db.get_subscribed_channel_ids()
            telegram_ids: set[int] = set()
            inserted = 0
            updated = 0

            for ch in dialogs:
                data = {
                    "id": ch["id"],
                    "access_hash": ch["access_hash"],
                    "title": ch["title"],
                    "username": ch["username"],
                    "photo_id": ch.get("photo_id"),
                    "date": ch.get("date"),
                    "participants_count": ch.get("participants_count"),
                    "broadcast": ch.get("broadcast", 0),
                    "megagroup": ch.get("megagroup", 0),
                    "verified": ch.get("verified", 0),
                    "restricted": ch.get("restricted", 0),
                    "scam": ch.get("scam", 0),
                    "fake": ch.get("fake", 0),
                    "created_at": now,
                    "updated_at": now,
                }
                telegram_ids.add(ch["id"])

                if db.upsert_channel(data):
                    inserted += 1
                    logger.info(f"Added: {ch['title']} (id={ch['id']})")
                else:
                    updated += 1

            # Mark missing as unsubscribed
            missing_ids = existing_ids - telegram_ids
            if missing_ids:
                count = db.mark_unsubscribed(missing_ids, now)
                logger.warning(f"Marked {count} channels as unsubscribed")
                for cid in missing_ids:
                    logger.warning(f"  - Channel ID {cid}")

            db.commit()

            logger.info("=" * 50)
            logger.info("Sync completed!")
            logger.info(f"  Added: {inserted}")
            logger.info(f"  Updated: {updated}")
            logger.info(f"  Unsubscribed: {len(missing_ids)}")
            logger.info(f"  Total: {len(telegram_ids)}")
            logger.info("=" * 50)


async def sync_channels_direct() -> None:
    """Sync channels using direct Telethon connection (fallback)."""
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
    from telethon.tl.types import Channel

    from config import API_ID, API_HASH, PHONE_NUMBER, SESSION_PATH, validate_config

    logger.info("Starting channel sync (direct connection)...")

    validate_config()

    # Run migrations
    DatabaseMigration().migrate()

    # Connect to Telegram
    logger.info("Connecting to Telegram...")
    client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH)

    try:
        await client.start(phone=PHONE_NUMBER)
        logger.info("Connected to Telegram")

        # Fetch broadcast channels only (with flood wait handling)
        logger.info("Fetching channels...")
        telegram_channels: list[Channel] = []

        try:
            async for dialog in client.iter_dialogs():
                if dialog.is_channel and isinstance(dialog.entity, Channel):
                    channel = dialog.entity
                    if channel.broadcast:
                        telegram_channels.append(channel)
                        logger.debug(f"Found: {channel.title} (@{channel.username or 'N/A'})")
        except FloodWaitError as e:
            logger.warning(f"FloodWait on iter_dialogs: waiting {e.seconds} seconds...")
            await asyncio.sleep(e.seconds + 1)
            # Retry after waiting
            async for dialog in client.iter_dialogs():
                if dialog.is_channel and isinstance(dialog.entity, Channel):
                    channel = dialog.entity
                    if channel.broadcast:
                        telegram_channels.append(channel)

        logger.info(f"Found {len(telegram_channels)} broadcast channels")

        # Download channel photos
        photos_dir = DATA_DIR / "photos"
        photos_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading channel photos...")

        for channel in telegram_channels:
            photo_path = photos_dir / f"{channel.id}.jpg"
            if not photo_path.exists():
                try:
                    result = await client.download_profile_photo(
                        channel, file=str(photo_path)
                    )
                    if result:
                        logger.info(f"Downloaded photo for {channel.title}")
                except Exception as e:
                    logger.warning(f"Could not download photo for {channel.title}: {e}")

        # Merge to database
        now = int(time.time())
        with Database() as db:
            existing_ids = db.get_subscribed_channel_ids()
            telegram_ids: set[int] = set()
            inserted = 0
            updated = 0

            for channel in telegram_channels:
                photo_id = None
                if hasattr(channel, "photo") and channel.photo:
                    photo_id = getattr(channel.photo, "photo_id", None)

                data = {
                    "id": channel.id,
                    "access_hash": channel.access_hash,
                    "title": channel.title,
                    "username": channel.username,
                    "photo_id": photo_id,
                    "date": int(channel.date.timestamp()) if channel.date else None,
                    "participants_count": channel.participants_count,
                    "broadcast": 1 if channel.broadcast else 0,
                    "megagroup": 1 if channel.megagroup else 0,
                    "verified": 1 if channel.verified else 0,
                    "restricted": 1 if channel.restricted else 0,
                    "scam": 1 if channel.scam else 0,
                    "fake": 1 if channel.fake else 0,
                    "created_at": now,
                    "updated_at": now,
                }
                telegram_ids.add(channel.id)

                if db.upsert_channel(data):
                    inserted += 1
                    logger.info(f"Added: {channel.title} (id={channel.id})")
                else:
                    updated += 1

            # Mark missing as unsubscribed
            missing_ids = existing_ids - telegram_ids
            if missing_ids:
                count = db.mark_unsubscribed(missing_ids, now)
                logger.warning(f"Marked {count} channels as unsubscribed")
                for cid in missing_ids:
                    logger.warning(f"  - Channel ID {cid}")

            db.commit()

            logger.info("=" * 50)
            logger.info("Sync completed!")
            logger.info(f"  Added: {inserted}")
            logger.info(f"  Updated: {updated}")
            logger.info(f"  Unsubscribed: {len(missing_ids)}")
            logger.info(f"  Total: {len(telegram_ids)}")
            logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await client.disconnect()
        logger.info("Disconnected from Telegram")


async def sync_channels() -> None:
    """Sync channels - uses daemon if available, else direct connection."""
    if await is_daemon_running():
        logger.info("TG daemon is running, using RPC")
        try:
            await sync_channels_via_daemon()
            return
        except TGClientConnectionError as e:
            logger.warning(f"Daemon connection failed: {e}")
            logger.info("Falling back to direct connection...")

    await sync_channels_direct()


if __name__ == "__main__":
    asyncio.run(sync_channels())
