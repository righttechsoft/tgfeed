"""Telegram daemon - centralized connection pool for TGFeed.

Manages multiple Telegram client connections and exposes them via RPC.
Run with: python tg_daemon.py
"""

import asyncio
import hashlib
import json
import logging
import signal
import sys
import time
from pathlib import Path

# Hash constants for backup matching
HASH_SIZE_THRESHOLD = 64 * 1024  # Files > 64KB use hash matching
HASH_CHUNK_SIZE = 64 * 1024  # Download first 64KB for hash

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import (
    Channel,
    InputChannel,
    Message,
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
    MessageMediaPoll,
)

from config import (
    DATA_DIR,
    MEDIA_DIR,
    SESSIONS_DIR,
    TG_DAEMON_HOST,
    TG_DAEMON_PORT,
)
from database import Database, DatabaseMigration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("telethon").setLevel(logging.WARNING)


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


def extract_entities(msg: Message) -> list[dict] | None:
    """Extract message entities as a list of dicts."""
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
    return entities if entities else None


def get_media_file_size(msg: Message) -> int | None:
    """Get file size of media in a message."""
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


def message_to_dict(msg: Message) -> dict:
    """Convert a Telegram message to a serializable dict."""
    fwd_from_id = None
    fwd_from_name = None
    if msg.fwd_from:
        if msg.fwd_from.from_id:
            fwd_from_id = getattr(msg.fwd_from.from_id, 'channel_id', None) or \
                          getattr(msg.fwd_from.from_id, 'user_id', None)
        fwd_from_name = msg.fwd_from.from_name

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
        "from_id": getattr(msg.from_id, 'user_id', None) if msg.from_id else None,
        "fwd_from_id": fwd_from_id,
        "fwd_from_name": fwd_from_name,
        "reply_to_msg_id": msg.reply_to.reply_to_msg_id if msg.reply_to else None,
        "media_type": get_media_type(msg.media),
        "views": msg.views,
        "forwards": msg.forwards,
        "replies": msg.replies.replies if msg.replies else None,
        "edit_date": int(msg.edit_date.timestamp()) if msg.edit_date else None,
        "post_author": msg.post_author,
        "grouped_id": msg.grouped_id,
        "has_media": msg.media is not None,
        "is_poll": isinstance(msg.media, MessageMediaPoll),
    }


def channel_to_dict(channel: Channel) -> dict:
    """Convert a Telegram Channel to a serializable dict."""
    photo_id = None
    if hasattr(channel, "photo") and channel.photo:
        photo_id = getattr(channel.photo, "photo_id", None)

    return {
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
    }


class ClientInfo:
    """Info about a connected Telegram client."""

    def __init__(self, cred_id: int, client: TelegramClient,
                 phone: str, is_primary: bool):
        self.cred_id = cred_id
        self.client = client
        self.phone = phone
        self.is_primary = is_primary
        self.connected = False
        self.last_used = time.time()

    def to_dict(self) -> dict:
        return {
            "id": self.cred_id,
            "phone": self.phone[:4] + "***" + self.phone[-2:] if len(self.phone) > 6 else "***",
            "connected": self.connected,
            "primary": self.is_primary,
            "last_used": self.last_used,
        }


class TelegramDaemon:
    """Main daemon class managing Telegram connections."""

    def __init__(self):
        self.clients: dict[int, ClientInfo] = {}  # cred_id -> ClientInfo
        self.primary_id: int | None = None
        self.server: asyncio.Server | None = None
        self.running = False

    async def start(self) -> None:
        """Start the daemon: load credentials, connect clients, start RPC server."""
        logger.info("Starting Telegram daemon...")

        # Run migrations to ensure tg_creds table exists
        DatabaseMigration().migrate()

        # Ensure sessions directory exists
        SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

        # Load and connect clients
        await self._load_and_connect_clients()

        if not self.clients:
            logger.error("No Telegram credentials found in database!")
            logger.info("Add credentials using: python -c \"from database import Database; ...")
            logger.info("Or run the migration script to import from .env")
            sys.exit(1)

        if self.primary_id is None:
            logger.warning("No primary credential set, using first available")
            self.primary_id = next(iter(self.clients.keys()))

        # Start RPC server
        self.running = True
        await self._start_rpc_server()

    async def _load_and_connect_clients(self) -> None:
        """Load credentials from database and connect all clients."""
        with Database() as db:
            creds = db.get_all_tg_creds()

        if not creds:
            logger.warning("No credentials in tg_creds table")
            return

        logger.info(f"Found {len(creds)} credential(s) in database")

        for cred in creds:
            cred_id = cred["id"]
            session_path = SESSIONS_DIR / f"{cred_id}.session"

            logger.info(f"Connecting client {cred_id} ({cred['phone_number'][:4]}***)")

            client = TelegramClient(
                str(session_path),
                cred["api_id"],
                cred["api_hash"]
            )

            info = ClientInfo(
                cred_id=cred_id,
                client=client,
                phone=cred["phone_number"],
                is_primary=cred["primary"]
            )

            try:
                await client.start(phone=cred["phone_number"])
                info.connected = True
                logger.info(f"  Client {cred_id} connected successfully")

                if cred["primary"]:
                    self.primary_id = cred_id
                    logger.info(f"  Client {cred_id} is PRIMARY")

            except Exception as e:
                logger.error(f"  Failed to connect client {cred_id}: {e}")
                info.connected = False

            self.clients[cred_id] = info

    async def _start_rpc_server(self) -> None:
        """Start the TCP RPC server."""
        self.server = await asyncio.start_server(
            self._handle_client,
            TG_DAEMON_HOST,
            TG_DAEMON_PORT
        )

        addr = self.server.sockets[0].getsockname()
        logger.info(f"RPC server listening on {addr[0]}:{addr[1]}")

        async with self.server:
            await self.server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """Handle a single RPC client connection."""
        addr = writer.get_extra_info('peername')
        logger.debug(f"New RPC connection from {addr}")

        try:
            while self.running:
                line = await reader.readline()
                if not line:
                    break

                try:
                    request = json.loads(line.decode())
                    response = await self._dispatch_rpc(request)
                except json.JSONDecodeError as e:
                    response = {"error": f"Invalid JSON: {e}"}
                except Exception as e:
                    logger.exception(f"RPC error: {e}")
                    response = {"error": str(e)}

                writer.write(json.dumps(response).encode() + b'\n')
                await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug(f"RPC connection closed from {addr}")

    async def _dispatch_rpc(self, request: dict) -> dict:
        """Dispatch an RPC request to the appropriate handler."""
        method = request.get("method", "")
        params = request.get("params", {})
        req_id = request.get("id")

        handler = getattr(self, f"_rpc_{method}", None)
        if handler is None:
            return {"id": req_id, "error": f"Unknown method: {method}"}

        try:
            result = await handler(**params)
            return {"id": req_id, "result": result}
        except FloodWaitError as e:
            # Return special flood_wait response so client can decide what to do
            logger.warning(f"FloodWaitError for {method}: must wait {e.seconds} seconds")
            return {
                "id": req_id,
                "error": "flood_wait",
                "flood_wait_seconds": e.seconds
            }
        except TypeError as e:
            return {"id": req_id, "error": f"Invalid params: {e}"}
        except Exception as e:
            logger.exception(f"Handler error for {method}")
            return {"id": req_id, "error": str(e)}

    def _get_client(self, client_id: int = None) -> TelegramClient:
        """Get a Telegram client by ID, or the primary client."""
        if client_id is not None:
            info = self.clients.get(client_id)
            if info and info.connected:
                info.last_used = time.time()
                return info.client
            raise ValueError(f"Client {client_id} not found or not connected")

        if self.primary_id is None:
            raise ValueError("No primary client available")

        info = self.clients.get(self.primary_id)
        if info and info.connected:
            info.last_used = time.time()
            return info.client

        raise ValueError("Primary client not connected")

    # RPC Handlers

    async def _rpc_ping(self) -> dict:
        """Health check."""
        connected = sum(1 for c in self.clients.values() if c.connected)
        return {
            "status": "ok",
            "clients": connected,
            "primary_id": self.primary_id,
        }

    async def _rpc_get_clients(self) -> list[dict]:
        """Get list of all clients."""
        return [info.to_dict() for info in self.clients.values()]

    async def _rpc_iter_dialogs(self, client_id: int = None) -> dict:
        """Get all dialogs (channels) from a client."""
        client = self._get_client(client_id)

        dialogs = []
        async for dialog in client.iter_dialogs():
            if dialog.is_channel and isinstance(dialog.entity, Channel):
                if dialog.entity.broadcast:  # Only broadcast channels
                    dialogs.append(channel_to_dict(dialog.entity))

        return {"dialogs": dialogs}

    async def _rpc_download_profile_photo(self, channel_id: int, access_hash: int,
                                          dest_path: str, client_id: int = None) -> dict:
        """Download a channel's profile photo."""
        client = self._get_client(client_id)

        input_channel = InputChannel(channel_id, access_hash)

        try:
            result = await client.download_profile_photo(input_channel, file=dest_path)
            return {"path": result}
        except Exception as e:
            logger.warning(f"Failed to download photo for {channel_id}: {e}")
            return {"path": None, "error": str(e)}

    async def _rpc_iter_messages(self, channel_id: int, access_hash: int,
                                 min_id: int = None, max_id: int = None,
                                 limit: int = None, reverse: bool = False,
                                 client_id: int = None) -> dict:
        """Get messages from a channel."""
        client = self._get_client(client_id)
        input_channel = InputChannel(channel_id, access_hash)

        kwargs = {}
        if min_id is not None:
            kwargs["min_id"] = min_id
        if max_id is not None:
            kwargs["max_id"] = max_id
        if limit is not None:
            kwargs["limit"] = limit
        if reverse:
            kwargs["reverse"] = reverse

        messages = []
        async for msg in client.iter_messages(input_channel, **kwargs):
            if isinstance(msg, Message):
                messages.append(message_to_dict(msg))

        return {"messages": messages}

    async def _rpc_get_messages(self, channel_id: int, access_hash: int,
                               ids: list[int], client_id: int = None) -> dict:
        """Get specific messages by ID."""
        client = self._get_client(client_id)
        input_channel = InputChannel(channel_id, access_hash)

        msgs = await client.get_messages(input_channel, ids=ids)
        messages = []
        for msg in msgs:
            if isinstance(msg, Message):
                messages.append(message_to_dict(msg))

        return {"messages": messages}

    async def _rpc_download_media(self, channel_id: int, access_hash: int,
                                  message_id: int, dest_dir: str,
                                  client_id: int = None) -> dict:
        """Download media from a message.

        Note: Backup handling is done in sync_history.py, not here.
        The daemon only handles direct Telegram downloads.
        """
        client = self._get_client(client_id)
        input_channel = InputChannel(channel_id, access_hash)

        # Get the message first
        msgs = await client.get_messages(input_channel, ids=[message_id])
        if not msgs or not msgs[0]:
            return {"path": None, "error": "Message not found"}

        msg = msgs[0]
        if not msg.media:
            return {"path": None, "error": "No media in message"}

        # Create destination directory
        dest_path = Path(dest_dir)
        channel_dest = dest_path / str(channel_id)
        channel_dest.mkdir(parents=True, exist_ok=True)

        # Download from Telegram
        try:
            path = await client.download_media(msg, file=channel_dest)
            if path:
                downloaded_path = Path(path)
                rel_path = f"{channel_id}/{downloaded_path.name}"
                return {"path": rel_path}
            return {"path": None, "error": "Download returned None"}
        except Exception as e:
            logger.error(f"Failed to download media: {e}")
            return {"path": None, "error": str(e)}

    async def _rpc_get_media_hash(self, channel_id: int, access_hash: int,
                                  message_id: int, client_id: int = None) -> dict:
        """Get hash of first 64KB of media for backup matching.

        For large files (>64KB), downloads only the first 64KB and computes MD5.
        For small files (<=64KB), returns size only (no hash needed).

        Returns:
            {
                "size": int,           # Total file size
                "hash": str | None,    # MD5 of first 64KB (None for small files)
                "needs_hash": bool     # Whether hash matching should be used
            }
        """
        client = self._get_client(client_id)
        input_channel = InputChannel(channel_id, access_hash)

        # Get the message
        msgs = await client.get_messages(input_channel, ids=[message_id])
        if not msgs or not msgs[0]:
            return {"error": "Message not found"}

        msg = msgs[0]
        if not msg.media:
            return {"error": "No media in message"}

        # Get file size
        file_size = get_media_file_size(msg)
        if file_size is None:
            return {"error": "Cannot determine file size"}

        # Small files don't need hash matching
        if file_size <= HASH_SIZE_THRESHOLD:
            return {
                "size": file_size,
                "hash": None,
                "needs_hash": False
            }

        # Download first 64KB and compute hash
        try:
            chunks = []
            bytes_read = 0

            async for chunk in client.iter_download(msg.media, limit=HASH_CHUNK_SIZE):
                chunks.append(chunk)
                bytes_read += len(chunk)
                if bytes_read >= HASH_CHUNK_SIZE:
                    break

            data = b''.join(chunks)[:HASH_CHUNK_SIZE]
            file_hash = hashlib.md5(data).hexdigest()

            return {
                "size": file_size,
                "hash": file_hash,
                "needs_hash": True
            }
        except Exception as e:
            logger.error(f"Failed to get media hash: {e}")
            return {"error": str(e)}

    async def _rpc_send_read_acknowledge(self, channel_id: int, access_hash: int,
                                         max_id: int, client_id: int = None) -> dict:
        """Mark messages as read in Telegram."""
        client = self._get_client(client_id)
        input_channel = InputChannel(channel_id, access_hash)

        try:
            await client.send_read_acknowledge(input_channel, max_id=max_id)
            return {"success": True}
        except Exception as e:
            logger.error(f"Failed to send read acknowledge: {e}")
            return {"success": False, "error": str(e)}

    async def _rpc_get_read_state(self, channel_id: int, access_hash: int,
                                  client_id: int = None) -> dict:
        """Get read state for a channel."""
        client = self._get_client(client_id)

        async for dialog in client.iter_dialogs():
            if dialog.entity.id == channel_id:
                return {"read_inbox_max_id": dialog.dialog.read_inbox_max_id}

        return {"read_inbox_max_id": None, "error": "Channel not found in dialogs"}

    async def shutdown(self) -> None:
        """Gracefully shut down the daemon."""
        logger.info("Shutting down daemon...")
        self.running = False

        # Stop the server
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        # Disconnect all clients
        for info in self.clients.values():
            if info.connected:
                try:
                    await info.client.disconnect()
                    logger.info(f"Disconnected client {info.cred_id}")
                except Exception as e:
                    logger.warning(f"Error disconnecting client {info.cred_id}: {e}")

        logger.info("Daemon shutdown complete")


async def main() -> None:
    """Main entry point."""
    daemon = TelegramDaemon()

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(daemon.shutdown())

    # Windows doesn't support add_signal_handler
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)
    else:
        # On Windows, Ctrl+C will raise KeyboardInterrupt
        pass

    try:
        await daemon.start()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Daemon error: {e}")
    finally:
        await daemon.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
