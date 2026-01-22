"""RPC client for communicating with the Telegram daemon."""

import asyncio
import json
import logging

from config import TG_DAEMON_HOST, TG_DAEMON_PORT

logger = logging.getLogger(__name__)


class TGClientError(Exception):
    """Error from TG daemon."""
    pass


class TGClientConnectionError(TGClientError):
    """Could not connect to daemon."""
    pass


class TGFloodWaitError(TGClientError):
    """Telegram rate limit - must wait before retrying.

    Attributes:
        seconds: Number of seconds to wait before retrying.
    """

    def __init__(self, seconds: int, message: str = None):
        self.seconds = seconds
        super().__init__(message or f"Flood wait: must wait {seconds} seconds")


class TGClient:
    """RPC client for talking to tg_daemon via TCP.

    Usage:
        async with TGClient() as client:
            dialogs = await client.iter_dialogs()
    """

    # 16MB buffer limit for large responses (dialogs, messages)
    BUFFER_LIMIT = 16 * 1024 * 1024

    def __init__(self, host: str = None, port: int = None):
        self.host = host or TG_DAEMON_HOST
        self.port = port or TG_DAEMON_PORT
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()
        self._request_id = 0

    async def __aenter__(self) -> "TGClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        """Connect to the daemon TCP server."""
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self.host, self.port, limit=self.BUFFER_LIMIT
            )
            logger.debug(f"Connected to TG daemon at {self.host}:{self.port}")
        except (ConnectionRefusedError, OSError) as e:
            raise TGClientConnectionError(
                f"Could not connect to TG daemon at {self.host}:{self.port}: {e}"
            ) from e

    async def close(self) -> None:
        """Close the connection."""
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

    @property
    def connected(self) -> bool:
        """Check if connected to daemon."""
        return self._writer is not None and not self._writer.is_closing()

    async def _call(self, method: str, **kwargs) -> dict:
        """Make an RPC call and return the result."""
        if not self.connected:
            raise TGClientConnectionError("Not connected to daemon")

        async with self._lock:
            self._request_id += 1
            request = {
                "id": self._request_id,
                "method": method,
                "params": kwargs
            }

            try:
                self._writer.write(json.dumps(request).encode() + b'\n')
                await self._writer.drain()

                response_line = await self._reader.readline()
                if not response_line:
                    raise TGClientConnectionError("Connection closed by daemon")

                response = json.loads(response_line.decode())

                if "error" in response:
                    # Check for flood_wait special error
                    if response["error"] == "flood_wait":
                        seconds = response.get("flood_wait_seconds", 60)
                        raise TGFloodWaitError(seconds)
                    raise TGClientError(response["error"])

                return response.get("result", {})

            except (ConnectionResetError, BrokenPipeError) as e:
                await self.close()
                raise TGClientConnectionError(f"Connection lost: {e}") from e

    # Connection management

    async def ping(self) -> dict:
        """Check if daemon is alive."""
        return await self._call("ping")

    async def get_clients(self) -> list[dict]:
        """Get list of connected Telegram clients."""
        return await self._call("get_clients")

    # Channel operations (uses primary client)

    async def iter_dialogs(self) -> list[dict]:
        """Get all dialogs from primary client.

        Returns list of dialog dicts with channel info.
        """
        result = await self._call("iter_dialogs")
        return result.get("dialogs", [])

    async def download_profile_photo(self, channel_id: int, access_hash: int,
                                     dest_path: str) -> dict:
        """Download channel profile photo.

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            dest_path: Destination file path

        Returns:
            {"path": str} or {"path": None} if no photo
        """
        return await self._call(
            "download_profile_photo",
            channel_id=channel_id,
            access_hash=access_hash,
            dest_path=dest_path
        )

    # Message operations

    async def iter_messages(self, channel_id: int, access_hash: int,
                           min_id: int = None, max_id: int = None,
                           limit: int = None, reverse: bool = False,
                           client_id: int = None) -> list[dict]:
        """Get messages from a channel.

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            min_id: Minimum message ID (exclusive)
            max_id: Maximum message ID (exclusive)
            limit: Maximum number of messages to return
            reverse: If True, iterate from oldest to newest
            client_id: Specific client ID to use (optional)

        Returns:
            List of message dicts
        """
        params = {
            "channel_id": channel_id,
            "access_hash": access_hash,
        }
        if min_id is not None:
            params["min_id"] = min_id
        if max_id is not None:
            params["max_id"] = max_id
        if limit is not None:
            params["limit"] = limit
        if reverse:
            params["reverse"] = reverse
        if client_id is not None:
            params["client_id"] = client_id

        result = await self._call("iter_messages", **params)
        return result.get("messages", [])

    async def get_messages(self, channel_id: int, access_hash: int,
                          ids: list[int], client_id: int = None) -> list[dict]:
        """Get specific messages by ID.

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            ids: List of message IDs to fetch
            client_id: Specific client ID to use (optional)

        Returns:
            List of message dicts
        """
        params = {
            "channel_id": channel_id,
            "access_hash": access_hash,
            "ids": ids,
        }
        if client_id is not None:
            params["client_id"] = client_id

        result = await self._call("get_messages", **params)
        return result.get("messages", [])

    async def download_media(self, channel_id: int, access_hash: int,
                            message_id: int, dest_dir: str,
                            client_id: int = None,
                            backup_path: str = None) -> dict:
        """Download media from a message.

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            message_id: Message ID containing media
            dest_dir: Destination directory for the file
            client_id: Specific client ID to use (optional)
            backup_path: Optional path to check for existing media before downloading

        Returns:
            {"path": str} with relative path, or {"path": None, "error": str}
            May include "from_backup": True if file was found in backup_path
        """
        params = {
            "channel_id": channel_id,
            "access_hash": access_hash,
            "message_id": message_id,
            "dest_dir": dest_dir,
        }
        if client_id is not None:
            params["client_id"] = client_id
        if backup_path is not None:
            params["backup_path"] = backup_path

        return await self._call("download_media", **params)

    # Read status

    async def send_read_acknowledge(self, channel_id: int, access_hash: int,
                                   max_id: int, client_id: int = None) -> dict:
        """Mark messages as read in Telegram.

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            max_id: Mark all messages up to this ID as read
            client_id: Specific client ID to use (optional)

        Returns:
            {"success": bool}
        """
        params = {
            "channel_id": channel_id,
            "access_hash": access_hash,
            "max_id": max_id,
        }
        if client_id is not None:
            params["client_id"] = client_id

        return await self._call("send_read_acknowledge", **params)

    async def get_read_state(self, channel_id: int, access_hash: int,
                            client_id: int = None) -> dict:
        """Get read state for a channel (inbox max read ID).

        Args:
            channel_id: Channel ID
            access_hash: Channel access hash
            client_id: Specific client ID to use (optional)

        Returns:
            {"read_inbox_max_id": int}
        """
        params = {
            "channel_id": channel_id,
            "access_hash": access_hash,
        }
        if client_id is not None:
            params["client_id"] = client_id

        return await self._call("get_read_state", **params)


# Convenience function to check daemon availability
async def is_daemon_running(host: str = None, port: int = None) -> bool:
    """Check if the TG daemon is running and responsive."""
    try:
        async with TGClient(host, port) as client:
            await client.ping()
            return True
    except TGClientConnectionError:
        return False
