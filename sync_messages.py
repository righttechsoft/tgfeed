"""Download messages from all subscribed Telegram channels.

Uses tg_daemon if available, falls back to direct Telethon connection.
"""

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

from config import MEDIA_DIR
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

# Concurrency settings for media downloads
CONCURRENT_DOWNLOADS = 5


async def sync_messages_via_daemon() -> None:
    """Sync messages using the TG daemon."""
    logger.info("Starting message sync via daemon...")

    # Run migrations
    DatabaseMigration().migrate()

    # Get active channels
    with Database() as db:
        channels = [dict(row) for row in db.get_active_channels()]

    if not channels:
        logger.warning("No active channels found. Set active=1 for channels to download.")
        return

    logger.info(f"Found {len(channels)} active channels")

    async with TGClient() as client:
        status = await client.ping()
        logger.info(f"Connected to daemon (clients: {status['clients']})")

        download_semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]

            logger.info(f"Processing channel: {channel_title} (id={channel_id})")

            with Database() as db:
                db.create_channel_messages_table(channel_id)
                db.commit()
                latest_id = db.get_latest_message_id(channel_id)
                is_first_sync = latest_id is None

                if latest_id:
                    logger.info(f"  Latest message ID in DB: {latest_id}")
                else:
                    logger.info("  No messages in DB, downloading only latest")

            try:
                # Phase 1: Fetch messages from daemon
                if is_first_sync:
                    logger.info("  Fetching latest message from Telegram...")
                    messages = await client.iter_messages(
                        channel_id, access_hash, limit=10
                    )
                    # Filter out polls and get only the latest non-poll
                    raw_messages = []
                    for msg in messages:
                        if not msg.get("is_poll", False):
                            raw_messages.append(msg)
                            logger.info(f"  Fetched message id={msg['id']}")
                            break
                else:
                    logger.info(f"  Fetching messages newer than id={latest_id}...")
                    messages = await client.iter_messages(
                        channel_id, access_hash, min_id=latest_id, reverse=True
                    )
                    raw_messages = [m for m in messages if not m.get("is_poll", False)]

                    if len(raw_messages) % 100 == 0 and len(raw_messages) > 0:
                        logger.info(f"  Fetched {len(raw_messages)} messages so far...")

                if not raw_messages:
                    logger.info("  No new messages")
                else:
                    logger.info(f"  Fetched {len(raw_messages)} messages, downloading media...")

                    # Phase 2: Download media concurrently
                    async def download_with_semaphore(msg: dict) -> tuple[int, str | None]:
                        async with download_semaphore:
                            if not msg.get("has_media") or msg.get("is_poll"):
                                return (msg["id"], None)
                            # Pass MEDIA_DIR - daemon adds channel_id subfolder
                            result = await client.download_media(
                                channel_id, access_hash, msg["id"], str(MEDIA_DIR)
                            )
                            path = result.get("path")
                            if path:
                                logger.info(f"    Downloaded media: {path}")
                            return (msg["id"], path)

                    tasks = [download_with_semaphore(msg) for msg in raw_messages]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    media_paths = {}
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"    Media download error: {result}")
                            continue
                        msg_id, path = result
                        if path:
                            media_paths[msg_id] = path

                    # Phase 3: Build message dicts and insert
                    collected_messages = []
                    media_count = 0
                    now = int(time.time())

                    for msg in raw_messages:
                        media_path = media_paths.get(msg["id"])
                        has_media = msg.get("has_media", False) and not msg.get("is_poll", False)

                        if media_path:
                            media_count += 1

                        # Convert daemon message format to database format
                        data = {
                            "id": msg["id"],
                            "date": msg.get("date"),
                            "message": msg.get("message"),
                            "entities": json.dumps(msg["entities"]) if msg.get("entities") else None,
                            "out": msg.get("out", 0),
                            "mentioned": msg.get("mentioned", 0),
                            "media_unread": msg.get("media_unread", 0),
                            "silent": msg.get("silent", 0),
                            "post": msg.get("post", 0),
                            "from_id": msg.get("from_id"),
                            "fwd_from_id": msg.get("fwd_from_id"),
                            "fwd_from_name": msg.get("fwd_from_name"),
                            "reply_to_msg_id": msg.get("reply_to_msg_id"),
                            "media_type": msg.get("media_type"),
                            "media_path": media_path,
                            "views": msg.get("views"),
                            "forwards": msg.get("forwards"),
                            "replies": msg.get("replies"),
                            "edit_date": msg.get("edit_date"),
                            "post_author": msg.get("post_author"),
                            "grouped_id": msg.get("grouped_id"),
                            "created_at": now,
                        }

                        if has_media and not media_path:
                            data["media_pending"] = 1
                            logger.warning(f"    Media download failed for message {msg['id']}, marked as pending")

                        collected_messages.append(data)

                    message_count = len(collected_messages)
                    if collected_messages:
                        with Database() as db:
                            db.insert_messages_batch(channel_id, collected_messages)
                            db.update_channel_last_active(channel_id, now)
                            db.commit()

                    logger.info(f"  Downloaded {message_count} new messages, {media_count} media files")

            except TGFloodWaitError as e:
                logger.warning(f"  FloodWait: must wait {e.seconds} seconds, skipping to next channel")
                continue
            except Exception as e:
                logger.error(f"  Error downloading messages: {e}")
                continue

            # Retry pending media downloads
            with Database() as db:
                pending_messages = db.get_messages_with_pending_media(channel_id, limit=10)

            if pending_messages:
                logger.info(f"  Retrying {len(pending_messages)} pending media downloads...")

                for pending in pending_messages:
                    # Pass MEDIA_DIR - daemon adds channel_id subfolder
                    result = await client.download_media(
                        channel_id, access_hash, pending["id"], str(MEDIA_DIR)
                    )
                    media_path = result.get("path")

                    with Database() as db:
                        if media_path:
                            db.update_message_media(channel_id, pending["id"], media_path, media_pending=0)
                            logger.info(f"    Message {pending['id']}: Downloaded media: {media_path}")
                        else:
                            logger.warning(f"    Message {pending['id']}: Media download still pending")
                        db.commit()

            # Sync read status from Telegram
            try:
                result = await client.get_read_state(channel_id, access_hash)
                read_max_id = result.get("read_inbox_max_id")

                if read_max_id:
                    with Database() as db:
                        updated = db.mark_messages_read_up_to(channel_id, read_max_id)
                        if updated > 0:
                            db.commit()
                            logger.info(f"  Synced read status: {updated} messages marked read (up to id={read_max_id})")
            except Exception as e:
                logger.warning(f"  Failed to sync read status: {e}")

        logger.info("=" * 50)
        logger.info("Message sync completed!")
        logger.info("=" * 50)


async def sync_messages_direct() -> None:
    """Sync messages using direct Telethon connection (fallback)."""
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError
    from telethon.tl.types import (
        Message,
        MessageMediaPhoto,
        MessageMediaDocument,
        MessageMediaPoll,
        InputChannel,
        PeerUser,
        PeerChannel,
        MessageEntityTextUrl,
        MessageEntityUrl,
        MessageEntityPre,
    )

    from config import API_ID, API_HASH, PHONE_NUMBER, SESSION_PATH, validate_config

    def get_media_type(media) -> str | None:
        if media is None:
            return None
        from telethon.tl.types import (
            MessageMediaWebPage, MessageMediaGeo, MessageMediaContact,
            MessageMediaGame, MessageMediaInvoice, MessageMediaVenue,
        )
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
        if isinstance(media, MessageMediaGeo):
            return "geo"
        if isinstance(media, MessageMediaContact):
            return "contact"
        if isinstance(media, MessageMediaGame):
            return "game"
        if isinstance(media, MessageMediaInvoice):
            return "invoice"
        if isinstance(media, MessageMediaVenue):
            return "venue"
        return "unknown"

    def get_peer_id(peer) -> int | None:
        if peer is None:
            return None
        if isinstance(peer, PeerUser):
            return peer.user_id
        if isinstance(peer, PeerChannel):
            return peer.channel_id
        if hasattr(peer, "user_id"):
            return peer.user_id
        if hasattr(peer, "channel_id"):
            return peer.channel_id
        return None

    def is_poll_message(msg: Message) -> bool:
        return isinstance(msg.media, MessageMediaPoll)

    def extract_entities(msg: Message) -> str | None:
        if not msg.entities:
            return None
        entities = []
        for entity in msg.entities:
            entity_data = {
                "offset": entity.offset,
                "length": entity.length,
                "type": type(entity).__name__,
            }
            if isinstance(entity, MessageEntityTextUrl):
                entity_data["url"] = entity.url
            elif isinstance(entity, MessageEntityUrl):
                if msg.message:
                    entity_data["url"] = msg.message[entity.offset:entity.offset + entity.length]
            elif isinstance(entity, MessageEntityPre):
                entity_data["language"] = entity.language
            entities.append(entity_data)
        return json.dumps(entities, ensure_ascii=False) if entities else None

    def message_to_dict(msg: Message, media_path: str | None = None) -> dict:
        now = int(time.time())
        fwd_from_id = None
        fwd_from_name = None
        if msg.fwd_from:
            if msg.fwd_from.from_id:
                fwd_from_id = get_peer_id(msg.fwd_from.from_id)
            fwd_from_name = msg.fwd_from.from_name
        reply_to_msg_id = None
        if msg.reply_to:
            reply_to_msg_id = msg.reply_to.reply_to_msg_id
        replies_count = None
        if msg.replies:
            replies_count = msg.replies.replies
        return {
            "id": msg.id,
            "date": int(msg.date.timestamp()) if msg.date else None,
            "message": msg.message,
            "entities": extract_entities(msg),
            "out": 1 if msg.out else 0,
            "mentioned": 1 if msg.mentioned else 0,
            "media_unread": 1 if msg.media_unread else 0,
            "silent": 1 if msg.silent else 0,
            "post": 1 if msg.post else 0,
            "from_id": get_peer_id(msg.from_id),
            "fwd_from_id": fwd_from_id,
            "fwd_from_name": fwd_from_name,
            "reply_to_msg_id": reply_to_msg_id,
            "media_type": get_media_type(msg.media),
            "media_path": media_path,
            "views": msg.views,
            "forwards": msg.forwards,
            "replies": replies_count,
            "edit_date": int(msg.edit_date.timestamp()) if msg.edit_date else None,
            "post_author": msg.post_author,
            "grouped_id": msg.grouped_id,
            "created_at": now,
        }

    def has_downloadable_media(msg: Message) -> bool:
        if not msg.media:
            return False
        if isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
            return True
        return False

    async def download_media(client: TelegramClient, msg: Message, channel_id: int) -> str | None:
        if not has_downloadable_media(msg):
            return None
        channel_media_dir = MEDIA_DIR / str(channel_id)
        channel_media_dir.mkdir(parents=True, exist_ok=True)
        try:
            path = await client.download_media(msg, file=channel_media_dir)
            if path:
                return str(Path(path).relative_to(MEDIA_DIR))
        except Exception as e:
            logger.error(f"    Failed to download media for message {msg.id}: {e}")
        return None

    logger.info("Starting message sync (direct connection)...")

    validate_config()
    DatabaseMigration().migrate()

    with Database() as db:
        channels = [dict(row) for row in db.get_active_channels()]

    if not channels:
        logger.warning("No active channels found.")
        return

    logger.info(f"Found {len(channels)} active channels")
    logger.info("Connecting to Telegram...")
    client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH)

    try:
        await client.start(phone=PHONE_NUMBER)
        logger.info("Connected to Telegram")

        download_semaphore = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

        for channel in channels:
            channel_id = channel["id"]
            channel_title = channel["title"]
            access_hash = channel["access_hash"]

            logger.info(f"Processing channel: {channel_title} (id={channel_id})")

            with Database() as db:
                db.create_channel_messages_table(channel_id)
                db.commit()
                latest_id = db.get_latest_message_id(channel_id)
                is_first_sync = latest_id is None

                if latest_id:
                    logger.info(f"  Latest message ID in DB: {latest_id}")
                else:
                    logger.info("  No messages in DB, downloading only latest")

            try:
                input_channel = InputChannel(channel_id, access_hash)
                raw_messages: list[Message] = []

                if is_first_sync:
                    logger.info("  Fetching latest message from Telegram...")
                    async for msg in client.iter_messages(input_channel, limit=10):
                        if isinstance(msg, Message) and not is_poll_message(msg):
                            raw_messages.append(msg)
                            logger.info(f"  Fetched message id={msg.id}")
                            break
                else:
                    logger.info(f"  Fetching messages newer than id={latest_id}...")
                    async for msg in client.iter_messages(input_channel, min_id=latest_id, reverse=True):
                        if isinstance(msg, Message) and not is_poll_message(msg):
                            raw_messages.append(msg)
                        if len(raw_messages) % 100 == 0 and len(raw_messages) > 0:
                            logger.info(f"  Fetched {len(raw_messages)} messages so far...")

                if not raw_messages:
                    logger.info("  No new messages")
                else:
                    logger.info(f"  Fetched {len(raw_messages)} messages, downloading media...")

                    messages_with_media = [(msg, idx) for idx, msg in enumerate(raw_messages) if has_downloadable_media(msg)]

                    media_paths: dict[int, str | None] = {}
                    if messages_with_media:
                        logger.info(f"  Downloading {len(messages_with_media)} media files...")

                        async def download_with_semaphore(msg: Message, idx: int) -> tuple[int, str | None]:
                            async with download_semaphore:
                                path = await download_media(client, msg, channel_id)
                                if path:
                                    logger.info(f"    Downloaded media: {path}")
                                return (idx, path)

                        tasks = [download_with_semaphore(msg, idx) for msg, idx in messages_with_media]
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"    Media download error: {result}")
                                continue
                            idx, path = result
                            media_paths[idx] = path

                    collected_messages = []
                    media_count = 0
                    for idx, msg in enumerate(raw_messages):
                        media_path = media_paths.get(idx)
                        has_media = has_downloadable_media(msg)
                        if media_path:
                            media_count += 1
                        data = message_to_dict(msg, media_path)
                        if has_media and not media_path:
                            data["media_pending"] = 1
                            logger.warning(f"    Media download failed for message {msg.id}, marked as pending")
                        collected_messages.append(data)

                    message_count = len(collected_messages)
                    if collected_messages:
                        with Database() as db:
                            db.insert_messages_batch(channel_id, collected_messages)
                            db.update_channel_last_active(channel_id, int(time.time()))
                            db.commit()

                    logger.info(f"  Downloaded {message_count} new messages, {media_count} media files")

            except FloodWaitError as e:
                logger.warning(f"  FloodWait: must wait {e.seconds} seconds, skipping to next channel")
                continue
            except Exception as e:
                logger.error(f"  Error downloading messages: {e}")
                continue

            # Retry pending media
            with Database() as db:
                pending_messages = db.get_messages_with_pending_media(channel_id, limit=10)

            if pending_messages:
                logger.info(f"  Retrying {len(pending_messages)} pending media downloads...")
                input_channel = InputChannel(channel_id, access_hash)
                for pending in pending_messages:
                    try:
                        messages = await client.get_messages(input_channel, ids=[pending["id"]])
                        if messages and messages[0]:
                            path = await download_media(client, messages[0], channel_id)
                            with Database() as db:
                                if path:
                                    db.update_message_media(channel_id, pending["id"], path, media_pending=0)
                                    logger.info(f"    Message {pending['id']}: Downloaded: {path}")
                                else:
                                    logger.warning(f"    Message {pending['id']}: Still pending")
                                db.commit()
                    except Exception as e:
                        logger.error(f"    Failed to retry message {pending['id']}: {e}")

            # Sync read status
            try:
                read_max_id = None
                async for dialog in client.iter_dialogs():
                    if hasattr(dialog.entity, 'id') and dialog.entity.id == channel_id:
                        read_max_id = dialog.dialog.read_inbox_max_id
                        break
                if read_max_id:
                    with Database() as db:
                        updated = db.mark_messages_read_up_to(channel_id, read_max_id)
                        if updated > 0:
                            db.commit()
                            logger.info(f"  Synced read status: {updated} messages marked read")
            except Exception as e:
                logger.warning(f"  Failed to sync read status: {e}")

        logger.info("=" * 50)
        logger.info("Message sync completed!")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await client.disconnect()
        logger.info("Disconnected from Telegram")


async def sync_messages() -> None:
    """Sync messages - uses daemon if available, else direct connection."""
    if await is_daemon_running():
        logger.info("TG daemon is running, using RPC")
        try:
            await sync_messages_via_daemon()
            return
        except TGClientConnectionError as e:
            logger.warning(f"Daemon connection failed: {e}")
            logger.info("Falling back to direct connection...")

    await sync_messages_direct()


if __name__ == "__main__":
    asyncio.run(sync_messages())
