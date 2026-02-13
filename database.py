"""Database module for TGFeed using SQLite."""

import logging
import sqlite3
import time

from config import DATABASE_PATH

logger = logging.getLogger(__name__)


class DatabaseMigration:
    """Handles database schema migrations."""

    def __init__(self, db_path=None) -> None:
        self.db_path = db_path or DATABASE_PATH

    def migrate(self) -> None:
        """Run all migrations."""
        logger.info("Running database migrations...")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            self._create_channels_table(cursor)
            self._add_column(cursor, "channels", "last_active", "INTEGER")
            self._add_column(cursor, "channels", "download_all", "INTEGER DEFAULT 0")
            self._add_column(cursor, "channels", "backup_path", "TEXT")
            # Media download flags (for non-download_all channels), default ON
            self._add_column(cursor, "channels", "download_images", "INTEGER DEFAULT 1")
            self._add_column(cursor, "channels", "download_videos", "INTEGER DEFAULT 1")
            self._add_column(cursor, "channels", "download_audio", "INTEGER DEFAULT 1")
            self._add_column(cursor, "channels", "download_other", "INTEGER DEFAULT 1")
            # Deduplication flag on groups - only process channels in groups with dedup=1
            self._add_column(cursor, "groups", "dedup", "INTEGER DEFAULT 0")
            self._migrate_channel_tables(cursor)
            conn.commit()
            logger.info("Database migrations completed")
        finally:
            conn.close()

    def _add_column(self, cursor, table: str, column: str, col_type: str) -> None:
        """Add a column to a table if it doesn't exist."""
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            logger.info(f"Added column {column} to {table}")

    def _migrate_channel_tables(self, cursor) -> None:
        """Add missing columns to all existing channel_* tables."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'channel_%'")
        tables = [row[0] for row in cursor.fetchall() if not row[0].startswith("channel_backup_hash_")]

        columns_to_add = [
            ("media_path", "TEXT"),
            ("entities", "TEXT"),
            ("read", "INTEGER DEFAULT 0"),
            ("read_at", "INTEGER"),  # Timestamp when message was marked as read
            ("rating", "INTEGER DEFAULT 0"),
            ("bookmarked", "INTEGER DEFAULT 0"),
            ("anchored", "INTEGER DEFAULT 0"),
            ("hidden", "INTEGER DEFAULT 0"),
            ("html_downloaded", "INTEGER DEFAULT 0"),
            ("media_pending", "INTEGER DEFAULT 0"),
            ("read_in_tg", "INTEGER DEFAULT 0"),
            ("video_thumbnail_path", "TEXT"),
            # Deduplication columns
            ("ai_summary", "TEXT"),
            ("content_hash", "TEXT"),
            ("content_hash_pending", "INTEGER DEFAULT 1"),
            ("duplicate_of_channel", "INTEGER"),
            ("duplicate_of_message", "INTEGER"),
            # Media deduplication columns
            ("media_hash", "TEXT"),
            ("media_hash_pending", "INTEGER DEFAULT 1"),
        ]

        for table_name in tables:
            for col, col_type in columns_to_add:
                self._add_column(cursor, table_name, col, col_type)

            # Add indexes for better query performance
            self._create_index_if_not_exists(cursor, table_name, "read_date", ["read", "date"])
            self._create_index_if_not_exists(cursor, table_name, "bookmarked", ["bookmarked"])
            self._create_index_if_not_exists(cursor, table_name, "anchored", ["anchored"])
            self._create_index_if_not_exists(cursor, table_name, "hidden", ["hidden"])
            self._create_index_if_not_exists(cursor, table_name, "content_hash", ["content_hash"])
            self._create_index_if_not_exists(cursor, table_name, "content_hash_pending", ["content_hash_pending"])
            self._create_index_if_not_exists(cursor, table_name, "media_hash", ["media_hash"])
            self._create_index_if_not_exists(cursor, table_name, "media_hash_pending", ["media_hash_pending"])

        # Create content_hashes lookup table for cross-channel deduplication
        self._create_content_hashes_table(cursor)

        # Create media_hashes lookup table for media deduplication
        self._create_media_hashes_table(cursor)

        # Create tg_creds table for Telegram daemon
        self._create_tg_creds_table(cursor)

        # Create tag_exclusions table for tag-based message exclusion
        self._create_tag_exclusions_table(cursor)

        # Create FTS5 table for full-text search
        self._create_fts_table(cursor)

    def _create_index_if_not_exists(self, cursor, table_name: str, index_suffix: str, columns: list[str]) -> None:
        """Create an index if it doesn't exist. Skips if any column doesn't exist."""
        index_name = f"idx_{table_name}_{index_suffix}"
        # Check that all columns exist before creating index
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_cols = {row[1] for row in cursor.fetchall()}
        for col in columns:
            if col not in existing_cols:
                return  # Skip index creation if column doesn't exist
        cols = ", ".join(columns)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({cols})")

    def _create_content_hashes_table(self, cursor) -> None:
        """Create the content_hashes lookup table if it doesn't exist."""
        # Migrate from old schema (hash TEXT PRIMARY KEY) to new (hash + group_id)
        cursor.execute("PRAGMA table_info(content_hashes)")
        cols = {row[1] for row in cursor.fetchall()}
        if cols and "group_id" not in cols:
            logger.info("Migrating content_hashes table to add group_id...")
            cursor.execute("DROP TABLE content_hashes")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS content_hashes (
                hash TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                message_date INTEGER,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (hash, group_id)
            )
        """)
        self._create_index_if_not_exists(cursor, "content_hashes", "date", ["message_date"])

    def _create_media_hashes_table(self, cursor) -> None:
        """Create the media_hashes lookup table if it doesn't exist."""
        # Migrate from old schema (hash TEXT PRIMARY KEY) to new (hash + group_id)
        cursor.execute("PRAGMA table_info(media_hashes)")
        cols = {row[1] for row in cursor.fetchall()}
        if cols and "group_id" not in cols:
            logger.info("Migrating media_hashes table to add group_id...")
            cursor.execute("DROP TABLE media_hashes")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS media_hashes (
                hash TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                message_date INTEGER,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (hash, group_id)
            )
        """)
        self._create_index_if_not_exists(cursor, "media_hashes", "date", ["message_date"])

    def _create_channels_table(self, cursor) -> None:
        """Create the channels table if it doesn't exist."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY,
                access_hash INTEGER,
                title TEXT NOT NULL,
                username TEXT,
                photo_id INTEGER,
                date INTEGER,
                participants_count INTEGER,
                broadcast INTEGER DEFAULT 0,
                megagroup INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                restricted INTEGER DEFAULT 0,
                scam INTEGER DEFAULT 0,
                fake INTEGER DEFAULT 0,
                subscribed INTEGER DEFAULT 1,
                active INTEGER DEFAULT 0,
                group_id INTEGER,
                created_at INTEGER,
                updated_at INTEGER
            )
        """)
        self._create_index_if_not_exists(cursor, "channels", "username", ["username"])
        self._create_index_if_not_exists(cursor, "channels", "subscribed", ["subscribed"])

        # Create groups table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
        """)

    def _create_tg_creds_table(self, cursor) -> None:
        """Create the tg_creds table for Telegram daemon credentials."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tg_creds (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                phone_number TEXT NOT NULL,
                "primary" INTEGER DEFAULT 0 NOT NULL
            )
        """)

    def _create_tag_exclusions_table(self, cursor) -> None:
        """Create the tag_exclusions table if it doesn't exist."""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tag_exclusions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tags TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            )
        """)

    def _create_fts_table(self, cursor) -> None:
        """Create FTS5 virtual table for full-text search with trigram tokenizer."""
        # Check if FTS5 table already exists
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'"
        )
        row = cursor.fetchone()
        if row is not None:
            # Check if it's the broken contentless version - need to drop and recreate
            if "content=''" in row[0].lower():
                logger.info("Dropping broken contentless FTS5 table (UNINDEXED columns don't work in contentless mode)...")
                cursor.execute("DROP TABLE messages_fts")
            else:
                # Already correct version, nothing to do
                return

        cursor.execute("""
            CREATE VIRTUAL TABLE messages_fts USING fts5(
                channel_id UNINDEXED,
                message_id UNINDEXED,
                message,
                tokenize="trigram"
            )
        """)
        logger.info("Created FTS5 search index table")


class _DebugCursor:
    """Wrapper around sqlite3.Cursor that logs queries and execution time."""

    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def execute(self, sql, parameters=()):
        short = sql.strip().replace('\n', ' ')
        short = ' '.join(short.split())[:200]
        start = time.perf_counter()
        result = self._cursor.execute(sql, parameters)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(f"[SQL {elapsed_ms:.1f}ms] {short}")
        return self

    def executemany(self, sql, seq_of_parameters):
        short = sql.strip().replace('\n', ' ')
        short = ' '.join(short.split())[:200]
        start = time.perf_counter()
        result = self._cursor.executemany(sql, seq_of_parameters)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(f"[SQL {elapsed_ms:.1f}ms] {short}")
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def description(self):
        return self._cursor.description


class Database:
    """Database connection and operations."""

    # Set to True to log all SQL queries with execution time
    DEBUG_QUERIES = False

    def __init__(self, db_path=None) -> None:
        self.db_path = db_path or DATABASE_PATH
        self.conn = None

    def __enter__(self) -> "Database":
        self.conn = sqlite3.connect(self.db_path, timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA cache_size=-65536")  # 64MB cache
        self.conn.execute("PRAGMA busy_timeout=10000")  # 10s busy timeout
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.conn:
            self.conn.close()

    def cursor(self) -> "_DebugCursor | sqlite3.Cursor":
        """Return a cursor, optionally wrapped for debug logging."""
        c = self.conn.cursor()
        if self.DEBUG_QUERIES:
            return _DebugCursor(c)
        return c

    def get_subscribed_channel_ids(self) -> set[int]:
        """Get all channel IDs currently marked as subscribed."""
        cursor = self.cursor()
        cursor.execute("SELECT id FROM channels WHERE subscribed = 1")
        return {row[0] for row in cursor.fetchall()}

    def get_subscribed_channels(self) -> list[sqlite3.Row]:
        """Get all channels currently marked as subscribed."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM channels WHERE subscribed = 1")
        return cursor.fetchall()

    def get_active_channels(self) -> list[sqlite3.Row]:
        """Get all channels marked as active for message downloading."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM channels WHERE active = 1")
        return cursor.fetchall()

    def get_dedup_channels(self) -> list[sqlite3.Row]:
        """Get all channels in groups with deduplication enabled."""
        cursor = self.cursor()
        cursor.execute("""
            SELECT c.* FROM channels c
            JOIN groups g ON c.group_id = g.id
            WHERE c.active = 1 AND g.dedup = 1
        """)
        return cursor.fetchall()

    def set_group_dedup(self, group_id: int, dedup: int) -> None:
        """Set the dedup flag for a group (0 or 1)."""
        cursor = self.cursor()
        cursor.execute("UPDATE groups SET dedup = ? WHERE id = ?", (dedup, group_id))

    def create_channel_messages_table(self, channel_id: int) -> None:
        """Create a messages table for a specific channel if it doesn't exist."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                date INTEGER,
                message TEXT,
                entities TEXT,
                out INTEGER DEFAULT 0,
                mentioned INTEGER DEFAULT 0,
                media_unread INTEGER DEFAULT 0,
                silent INTEGER DEFAULT 0,
                post INTEGER DEFAULT 0,
                from_id INTEGER,
                fwd_from_id INTEGER,
                fwd_from_name TEXT,
                reply_to_msg_id INTEGER,
                media_type TEXT,
                media_path TEXT,
                views INTEGER,
                forwards INTEGER,
                replies INTEGER,
                edit_date INTEGER,
                post_author TEXT,
                grouped_id INTEGER,
                created_at INTEGER,
                read INTEGER DEFAULT 0,
                read_at INTEGER,
                rating INTEGER DEFAULT 0,
                bookmarked INTEGER DEFAULT 0,
                anchored INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0,
                html_downloaded INTEGER DEFAULT 0,
                media_pending INTEGER DEFAULT 0,
                read_in_tg INTEGER DEFAULT 0,
                video_thumbnail_path TEXT,
                ai_summary TEXT,
                content_hash TEXT,
                content_hash_pending INTEGER DEFAULT 1,
                duplicate_of_channel INTEGER,
                duplicate_of_message INTEGER,
                media_hash TEXT,
                media_hash_pending INTEGER DEFAULT 1
            )
        """)

        # Create indexes
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_read_date ON {table_name} (read, date)
        """)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_bookmarked ON {table_name} (bookmarked)
        """)

    def get_latest_message_id(self, channel_id: int) -> int | None:
        """Get the latest message ID for a channel, or None if no messages."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"SELECT MAX(id) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
        except sqlite3.Error:
            return None

    def get_oldest_message_id(self, channel_id: int) -> int | None:
        """Get the oldest message ID for a channel, or None if no messages."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"SELECT MIN(id) FROM {table_name}")
            result = cursor.fetchone()
            return result[0] if result and result[0] else None
        except sqlite3.Error:
            return None

    def insert_message(self, channel_id: int, data: dict) -> None:
        """Insert a message into the channel's messages table."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            INSERT OR IGNORE INTO {table_name} (
                id, date, message, entities, out, mentioned, media_unread, silent, post,
                from_id, fwd_from_id, fwd_from_name, reply_to_msg_id, media_type,
                media_path, views, forwards, replies, edit_date, post_author, grouped_id, created_at,
                media_pending, read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["id"], data["date"], data["message"], data["entities"], data["out"],
            data["mentioned"], data["media_unread"], data["silent"], data["post"],
            data["from_id"], data["fwd_from_id"], data["fwd_from_name"],
            data["reply_to_msg_id"], data["media_type"], data["media_path"],
            data["views"], data["forwards"], data["replies"], data["edit_date"],
            data["post_author"], data["grouped_id"], data["created_at"],
            data.get("media_pending", 0),
            data.get("read", 0),
        ))

    def insert_messages_batch(self, channel_id: int, messages: list[dict]) -> int:
        """Insert multiple messages in a single transaction. Returns count inserted."""
        if not messages:
            return 0
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        values = [(
            d["id"], d["date"], d["message"], d["entities"], d.get("out", 0),
            d.get("mentioned", 0), d.get("media_unread", 0), d.get("silent", 0), d.get("post", 0),
            d.get("from_id"), d.get("fwd_from_id"), d.get("fwd_from_name"),
            d.get("reply_to_msg_id"), d.get("media_type"), d.get("media_path"),
            d.get("views"), d.get("forwards"), d.get("replies"), d.get("edit_date"),
            d.get("post_author"), d.get("grouped_id"), d.get("created_at"),
            d.get("media_pending", 0),
            d.get("read", 0),
        ) for d in messages]

        cursor.executemany(f"""
            INSERT OR IGNORE INTO {table_name} (
                id, date, message, entities, out, mentioned, media_unread, silent, post,
                from_id, fwd_from_id, fwd_from_name, reply_to_msg_id, media_type,
                media_path, views, forwards, replies, edit_date, post_author, grouped_id, created_at,
                media_pending, read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
        return cursor.rowcount

    def get_messages_with_pending_media(self, channel_id: int, limit: int = 10) -> list[dict]:
        """Get messages that have pending media downloads."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, media_type FROM {table_name}
                WHERE media_pending = 1 AND media_path IS NULL
                ORDER BY date DESC
                LIMIT ?
            """, (limit,))
            return [{"id": row[0], "media_type": row[1]} for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def update_message_media(self, channel_id: int, message_id: int, media_path: str | None, media_pending: int = 0) -> None:
        """Update media path and pending status for a message."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                UPDATE {table_name} SET media_path = ?, media_pending = ? WHERE id = ?
            """, (media_path, media_pending, message_id))
        except sqlite3.Error:
            pass

    def get_videos_without_thumbnails(self, channel_id: int, limit: int = 10) -> list[dict]:
        """Get video messages that don't have thumbnails, newest first."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, media_path FROM {table_name}
                WHERE media_type = 'video'
                  AND media_path IS NOT NULL
                  AND video_thumbnail_path IS NULL
                ORDER BY date DESC
                LIMIT ?
            """, (limit,))
            return [{"id": row[0], "media_path": row[1]} for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def update_video_thumbnail(self, channel_id: int, message_id: int, thumbnail_path: str) -> None:
        """Set the thumbnail path for a video message."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                UPDATE {table_name} SET video_thumbnail_path = ? WHERE id = ?
            """, (thumbnail_path, message_id))
        except sqlite3.Error:
            pass

    def upsert_channel(self, data: dict) -> bool:
        """Insert or update a channel. Returns True if inserted, False if updated."""
        cursor = self.cursor()
        cursor.execute("SELECT id FROM channels WHERE id = ?", (data["id"],))
        exists = cursor.fetchone() is not None

        if exists:
            cursor.execute("""
                UPDATE channels SET
                    access_hash = ?, title = ?, username = ?, photo_id = ?,
                    date = ?, participants_count = ?, broadcast = ?, megagroup = ?,
                    verified = ?, restricted = ?, scam = ?, fake = ?,
                    subscribed = 1, updated_at = ?
                WHERE id = ?
            """, (
                data["access_hash"], data["title"], data["username"], data["photo_id"],
                data["date"], data["participants_count"], data["broadcast"], data["megagroup"],
                data["verified"], data["restricted"], data["scam"], data["fake"],
                data["updated_at"], data["id"],
            ))
        else:
            cursor.execute("""
                INSERT INTO channels (
                    id, access_hash, title, username, photo_id, date,
                    participants_count, broadcast, megagroup, verified,
                    restricted, scam, fake, subscribed, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, (
                data["id"], data["access_hash"], data["title"], data["username"],
                data["photo_id"], data["date"], data["participants_count"],
                data["broadcast"], data["megagroup"], data["verified"],
                data["restricted"], data["scam"], data["fake"],
                data["created_at"], data["updated_at"],
            ))
        return not exists

    def mark_unsubscribed(self, channel_ids: set[int], timestamp: int) -> int:
        """Mark channels as unsubscribed. Returns count of affected rows."""
        if not channel_ids:
            return 0
        cursor = self.cursor()
        placeholders = ",".join("?" * len(channel_ids))
        cursor.execute(f"""
            UPDATE channels SET subscribed = 0, updated_at = ?
            WHERE id IN ({placeholders}) AND subscribed = 1
        """, (timestamp, *channel_ids))
        return cursor.rowcount

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    # Web UI methods

    def get_all_channels_with_groups(self) -> list[sqlite3.Row]:
        """Get all channels with their group info."""
        cursor = self.cursor()
        cursor.execute("""
            SELECT c.*, g.name as group_name
            FROM channels c
            LEFT JOIN groups g ON c.group_id = g.id
            ORDER BY g.name, c.title
        """)
        return cursor.fetchall()

    def get_all_groups(self) -> list[sqlite3.Row]:
        """Get all groups."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM groups ORDER BY name")
        return cursor.fetchall()

    def create_group(self, name: str) -> int:
        """Create a new group and return its ID."""
        cursor = self.cursor()
        cursor.execute("INSERT INTO groups (name) VALUES (?)", (name,))
        return cursor.lastrowid

    def rename_group(self, group_id: int, name: str) -> None:
        """Rename a group."""
        cursor = self.cursor()
        cursor.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))

    def delete_group(self, group_id: int) -> None:
        """Delete a group and unassign all its channels."""
        cursor = self.cursor()
        cursor.execute("UPDATE channels SET group_id = NULL WHERE group_id = ?", (group_id,))
        cursor.execute("DELETE FROM groups WHERE id = ?", (group_id,))

    def get_group_channel_count(self, group_id: int) -> int:
        """Get the number of channels in a group."""
        cursor = self.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM channels WHERE group_id = ?",
            (group_id,)
        )
        return cursor.fetchone()[0]

    def update_channel_active(self, channel_id: int, active: int) -> None:
        """Update channel active status."""
        cursor = self.cursor()
        cursor.execute(
            "UPDATE channels SET active = ? WHERE id = ?",
            (active, channel_id)
        )

    def update_channel_last_active(self, channel_id: int, timestamp: int) -> None:
        """Update channel last_active timestamp."""
        cursor = self.cursor()
        cursor.execute(
            "UPDATE channels SET last_active = ? WHERE id = ?",
            (timestamp, channel_id)
        )

    def update_channel_group(self, channel_id: int, group_id: int | None) -> None:
        """Update channel group."""
        cursor = self.cursor()
        cursor.execute(
            "UPDATE channels SET group_id = ? WHERE id = ?",
            (group_id, channel_id)
        )

    def update_channel_download_all(self, channel_id: int, download_all: int) -> None:
        """Update channel download_all status."""
        cursor = self.cursor()
        cursor.execute(
            "UPDATE channels SET download_all = ? WHERE id = ?",
            (download_all, channel_id)
        )

    def update_channel_backup_path(self, channel_id: int, backup_path: str | None) -> None:
        """Update channel backup_path for local media lookup."""
        cursor = self.cursor()
        cursor.execute(
            "UPDATE channels SET backup_path = ? WHERE id = ?",
            (backup_path, channel_id)
        )

    def update_channel_media_settings(self, channel_id: int, images: int, videos: int,
                                       audio: int, other: int) -> None:
        """Update channel media download settings."""
        cursor = self.cursor()
        cursor.execute("""
            UPDATE channels SET
                download_images = ?, download_videos = ?,
                download_audio = ?, download_other = ?
            WHERE id = ?
        """, (images, videos, audio, other, channel_id))

    def get_channel_media_settings(self, channel_id: int) -> dict:
        """Get channel media download settings."""
        cursor = self.cursor()
        cursor.execute("""
            SELECT download_images, download_videos, download_audio, download_other,
                   download_all
            FROM channels WHERE id = ?
        """, (channel_id,))
        row = cursor.fetchone()
        if row:
            return {
                "download_images": row[0] if row[0] is not None else 1,
                "download_videos": row[1] if row[1] is not None else 1,
                "download_audio": row[2] if row[2] is not None else 1,
                "download_other": row[3] if row[3] is not None else 1,
                "download_all": row[4] or 0,
            }
        return {"download_images": 1, "download_videos": 1, "download_audio": 1,
                "download_other": 1, "download_all": 0}

    def get_channel_by_id(self, channel_id: int) -> dict | None:
        """Get a channel by ID."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_channel_backup_path(self, channel_id: int) -> str | None:
        """Get the backup_path for a channel."""
        cursor = self.cursor()
        cursor.execute("SELECT backup_path FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        return row[0] if row else None

    def get_download_all_channels(self) -> list[sqlite3.Row]:
        """Get all channels with download_all enabled."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM channels WHERE active = 1 AND download_all = 1")
        return cursor.fetchall()

    def get_channels_by_group(self, group_id: int) -> list[sqlite3.Row]:
        """Get all channels in a group."""
        cursor = self.cursor()
        cursor.execute(
            "SELECT * FROM channels WHERE group_id = ?",
            (group_id,)
        )
        return cursor.fetchall()

    def get_group_tag_counts(self, group_id: int) -> dict[str, int]:
        """Count how many unread messages in the group contain each AI tag.

        Returns dict of lowercase tag -> message count.
        """
        channels = self.get_channels_by_group(group_id)
        channels = [c for c in channels if c["active"] == 1]

        tag_counts: dict[str, int] = {}
        cursor = self.cursor()
        for channel in channels:
            table_name = f"channel_{channel['id']}"
            try:
                cursor.execute(f"""
                    SELECT ai_summary FROM {table_name}
                    WHERE ai_summary IS NOT NULL
                      AND (read = 0 OR read IS NULL)
                      AND (hidden = 0 OR hidden IS NULL)
                """)
                for row in cursor.fetchall():
                    for tag in row[0].split(','):
                        tag = tag.strip().lower()
                        if tag:
                            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            except sqlite3.Error:
                pass

        return tag_counts

    def get_unread_messages_by_group(self, group_id: int, limit: int = 100, channel_id: int | None = None) -> list[dict]:
        """Get unread messages from all active channels in a group, sorted by date."""
        channels = self.get_channels_by_group(group_id)
        if not channels:
            return []

        # Filter to only active channels
        channels = [c for c in channels if c["active"] == 1]

        if channel_id is not None:
            channels = [c for c in channels if c["id"] == channel_id]
            if not channels:
                return []

        raw_messages = []
        cursor = self.cursor()
        for channel in channels:
            ch_id = channel["id"]
            table_name = f"channel_{ch_id}"
            try:
                cursor.execute(f"""
                    SELECT *, ? as channel_id, ? as channel_title, ? as channel_username
                    FROM {table_name}
                    WHERE (read = 0 OR read IS NULL)
                      AND (hidden = 0 OR hidden IS NULL)
                    ORDER BY date ASC
                    LIMIT ?
                """, (ch_id, channel["title"], channel["username"], limit * 3))
                for row in cursor.fetchall():
                    raw_messages.append(dict(row))
            except sqlite3.Error:
                pass

        return self._group_album_messages(raw_messages, limit, oldest_first=True)

    def get_earlier_messages_by_group(self, group_id: int, before_date: int, limit: int = 50, channel_id: int | None = None) -> list[dict]:
        """Get earlier (read) messages from all active channels in a group, older than before_date."""
        channels = self.get_channels_by_group(group_id)
        if not channels:
            return []

        # Filter to only active channels
        channels = [c for c in channels if c["active"] == 1]

        if channel_id is not None:
            channels = [c for c in channels if c["id"] == channel_id]
            if not channels:
                return []

        raw_messages = []
        cursor = self.cursor()
        for channel in channels:
            ch_id = channel["id"]
            table_name = f"channel_{ch_id}"
            try:
                cursor.execute(f"""
                    SELECT *, ? as channel_id, ? as channel_title, ? as channel_username
                    FROM {table_name}
                    WHERE date < ?
                      AND (hidden = 0 OR hidden IS NULL)
                    ORDER BY date DESC
                    LIMIT ?
                """, (ch_id, channel["title"], channel["username"], before_date, limit * 3))
                for row in cursor.fetchall():
                    raw_messages.append(dict(row))
            except sqlite3.Error:
                pass

        return self._group_album_messages(raw_messages, limit)

    def get_oldest_messages(self, channel_id: int, limit: int = 50) -> list[dict]:
        """Get the oldest messages for a channel, ordered oldest first."""
        cursor = self.cursor()
        cursor.execute("SELECT title, username FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        if not row:
            return []
        channel_title = row[0]
        channel_username = row[1]

        table_name = f"channel_{channel_id}"
        raw_messages = []
        try:
            cursor.execute(f"""
                SELECT *, ? as channel_id, ? as channel_title, ? as channel_username
                FROM {table_name}
                WHERE hidden = 0 OR hidden IS NULL
                ORDER BY date ASC
                LIMIT ?
            """, (channel_id, channel_title, channel_username, limit * 3))
            for row in cursor.fetchall():
                raw_messages.append(dict(row))
        except sqlite3.Error:
            pass

        return self._group_album_messages(raw_messages, limit, reverse=False)

    def get_later_messages(self, channel_id: int, after_date: int, limit: int = 50) -> list[dict]:
        """Get messages newer than after_date for a channel, ordered oldest first."""
        cursor = self.cursor()
        cursor.execute("SELECT title, username FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        if not row:
            return []
        channel_title = row[0]
        channel_username = row[1]

        table_name = f"channel_{channel_id}"
        raw_messages = []
        try:
            cursor.execute(f"""
                SELECT *, ? as channel_id, ? as channel_title, ? as channel_username
                FROM {table_name}
                WHERE date > ?
                  AND (hidden = 0 OR hidden IS NULL)
                ORDER BY date ASC
                LIMIT ?
            """, (channel_id, channel_title, channel_username, after_date, limit * 3))
            for row in cursor.fetchall():
                raw_messages.append(dict(row))
        except sqlite3.Error:
            pass

        return self._group_album_messages(raw_messages, limit, reverse=False)

    def _group_album_messages(self, raw_messages: list[dict], limit: int, reverse: bool = False, oldest_first: bool = False) -> list[dict]:
        """Group messages by (channel_id, grouped_id) into albums.

        Args:
            raw_messages: List of message dicts to group
            limit: Maximum number of messages to return
            reverse: If True, return newest first (for bookmarks). Default False (oldest first, for feeds).
            oldest_first: If True, keep oldest messages when limiting (for unread). Default False (keep newest).
        """
        grouped = {}  # key: (channel_id, grouped_id)
        ungrouped = []

        for msg in raw_messages:
            gid = msg.get("grouped_id")
            ch_id = msg.get("channel_id")
            if gid and ch_id:
                key = (ch_id, gid)
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(msg)
            else:
                ungrouped.append(msg)

        combined_messages = []

        for (ch_id, gid), group_msgs in grouped.items():
            group_msgs.sort(key=lambda m: m.get("id") or 0)
            base = group_msgs[0].copy()

            for m in group_msgs:
                if m.get("message"):
                    base["message"] = m["message"]
                    base["entities"] = m.get("entities")
                    break

            media_items = []
            for m in group_msgs:
                # Include items with media_path OR media_type (for download button)
                if m.get("media_path") or m.get("media_type"):
                    media_items.append({
                        "path": m.get("media_path"),
                        "type": m.get("media_type"),
                        "message_id": m["id"],
                        "video_thumbnail_path": m.get("video_thumbnail_path")
                    })

            base["media_items"] = media_items
            base["is_album"] = True
            base["album_message_ids"] = [m["id"] for m in group_msgs]
            combined_messages.append(base)

        for msg in ungrouped:
            # Include items with media_path OR media_type (for download button)
            if msg.get("media_path") or msg.get("media_type"):
                msg["media_items"] = [{
                    "path": msg.get("media_path"),
                    "type": msg.get("media_type"),
                    "message_id": msg["id"],
                    "video_thumbnail_path": msg.get("video_thumbnail_path")
                }]
            else:
                msg["media_items"] = []
            msg["is_album"] = False
            msg["album_message_ids"] = [msg["id"]]
            combined_messages.append(msg)

        if oldest_first:
            # For unread: sort ASC to keep oldest messages when limiting
            combined_messages.sort(key=lambda m: m.get("date") or 0)
            result = combined_messages[:limit]
            # Already sorted oldest first for display
        else:
            # Default: sort DESC to keep newest messages when limiting
            combined_messages.sort(key=lambda m: m.get("date") or 0, reverse=True)
            result = combined_messages[:limit]
            if not reverse:
                # Re-sort oldest first for display (scrolling feeds)
                result.sort(key=lambda m: m.get("date") or 0)
            # If reverse=True, keep newest first (for bookmarks)
        return result

    def mark_message_read(self, channel_id: int, message_id: int) -> None:
        """Mark a message as read."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"UPDATE {table_name} SET read = 1 WHERE id = ?", (message_id,))
        except sqlite3.Error:
            pass

    def get_message(self, channel_id: int, message_id: int) -> dict | None:
        """Get a single message by channel and message ID."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name} WHERE id = ?", (message_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        except sqlite3.Error:
            pass

    def get_message_duplicates(self, channel_id: int, message_id: int) -> list[dict]:
        """Get all messages that are duplicates of the given message.

        Only searches channels in the same group as the given channel.
        Returns list of dicts with channel_id, message_id, channel_title, and full message data.
        """
        cursor = self.cursor()

        # Get the group_id for this channel
        cursor.execute("SELECT group_id FROM channels WHERE id = ?", (channel_id,))
        row = cursor.fetchone()
        if not row or not row[0]:
            return []
        group_id = row[0]

        # Get channels in the same group
        cursor.execute(
            "SELECT id, title, username FROM channels WHERE group_id = ?",
            (group_id,)
        )
        group_channels = {r[0]: {"title": r[1], "username": r[2]} for r in cursor.fetchall()}

        duplicates = []

        for ch_id, info in group_channels.items():
            table_name = f"channel_{ch_id}"
            try:
                # Find messages pointing to the given message as their original
                cursor.execute(f"""
                    SELECT * FROM {table_name}
                    WHERE duplicate_of_channel = ? AND duplicate_of_message = ?
                      AND (hidden = 0 OR hidden IS NULL)
                """, (channel_id, message_id))

                for row in cursor.fetchall():
                    msg = dict(row)
                    msg["channel_id"] = ch_id
                    msg["channel_title"] = info["title"]
                    msg["channel_username"] = info["username"]
                    duplicates.append(msg)
            except (sqlite3.Error, ValueError):
                pass

        return duplicates

    def get_all_duplicates_for_group(self, group_id: int) -> dict[tuple[int, int], list[dict]]:
        """Get all duplicate relationships within a group in one batch.

        Returns a mapping from (original_channel_id, original_message_id) to list of
        duplicate messages pointing to that original. One query per channel table
        instead of per-message.
        """
        cursor = self.cursor()

        cursor.execute(
            "SELECT id, title, username FROM channels WHERE group_id = ?",
            (group_id,)
        )
        group_channels = {r[0]: {"title": r[1], "username": r[2]} for r in cursor.fetchall()}

        result: dict[tuple[int, int], list[dict]] = {}

        for ch_id, info in group_channels.items():
            table_name = f"channel_{ch_id}"
            try:
                cursor.execute(f"""
                    SELECT * FROM {table_name}
                    WHERE duplicate_of_channel IS NOT NULL
                      AND (hidden = 0 OR hidden IS NULL)
                """)
                for row in cursor.fetchall():
                    msg = dict(row)
                    msg["channel_id"] = ch_id
                    msg["channel_title"] = info["title"]
                    msg["channel_username"] = info["username"]
                    key = (msg["duplicate_of_channel"], msg["duplicate_of_message"])
                    if key not in result:
                        result[key] = []
                    result[key].append(msg)
            except (sqlite3.Error, ValueError):
                pass

        return result

    def mark_messages_read(self, messages: list[tuple[int, int]]) -> None:
        """Mark multiple messages as read. Each tuple is (channel_id, message_id)."""
        if not messages:
            return

        by_channel: dict[int, list[int]] = {}
        for channel_id, message_id in messages:
            if channel_id not in by_channel:
                by_channel[channel_id] = []
            by_channel[channel_id].append(message_id)

        logger.info(f"[mark_messages_read] {len(messages)} messages across {len(by_channel)} channels")

        now = int(time.time())
        cursor = self.cursor()
        for channel_id, message_ids in by_channel.items():
            table_name = f"channel_{channel_id}"
            try:
                start = time.time()
                placeholders = ",".join("?" * len(message_ids))
                # Set read=1 and read_at timestamp only if not already read
                cursor.execute(
                    f"UPDATE {table_name} SET read = 1, read_at = ? WHERE id IN ({placeholders}) AND read = 0",
                    (now, *message_ids)
                )
                elapsed = time.time() - start
                if elapsed > 0.1:
                    logger.info(f"[mark_messages_read] {table_name}: {len(message_ids)} msgs in {elapsed:.3f}s")
            except sqlite3.Error as e:
                logger.warning(f"[mark_messages_read] {table_name} error: {e}")

    def update_message_rating(self, channel_id: int, message_id: int, rating: int) -> None:
        """Update message rating (-1, 0, or 1)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"UPDATE {table_name} SET rating = ? WHERE id = ?", (rating, message_id))
        except sqlite3.Error:
            pass

    def update_message_bookmark(self, channel_id: int, message_id: int, bookmarked: int) -> None:
        """Update message bookmark status (0 or 1)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"UPDATE {table_name} SET bookmarked = ? WHERE id = ?", (bookmarked, message_id))
        except sqlite3.Error:
            pass

    def update_message_anchor(self, channel_id: int, message_id: int, anchored: int) -> None:
        """Update message anchor status (0 or 1)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"UPDATE {table_name} SET anchored = ? WHERE id = ?", (anchored, message_id))
        except sqlite3.Error:
            pass

    def update_message_hidden(self, channel_id: int, message_id: int, hidden: int) -> None:
        """Update message hidden status (0 or 1)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"UPDATE {table_name} SET hidden = ? WHERE id = ?", (hidden, message_id))
        except sqlite3.Error:
            pass

    def get_anchored_messages(self, channel_id: int) -> list[dict]:
        """Get all anchored messages for a channel, sorted by date."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, date, message, media_type, media_path, video_thumbnail_path
                FROM {table_name}
                WHERE anchored = 1
                  AND (hidden = 0 OR hidden IS NULL)
                ORDER BY date ASC
            """)
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def get_all_bookmarked_messages(self, limit: int = 100) -> list[dict]:
        """Get all bookmarked messages from all channels, sorted by date descending."""
        cursor = self.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'channel_%'")
        tables = [row[0] for row in cursor.fetchall() if not row[0].startswith("channel_backup_hash_")]

        cursor.execute("SELECT id, title, username FROM channels")
        channel_info = {row[0]: {"title": row[1], "username": row[2]} for row in cursor.fetchall()}

        raw_messages = []
        for table_name in tables:
            try:
                channel_id = int(table_name.replace("channel_", ""))
                info = channel_info.get(channel_id, {"title": "Unknown", "username": None})
                cursor.execute(f"""
                    SELECT *, ? as channel_id, ? as channel_title, ? as channel_username
                    FROM {table_name}
                    WHERE bookmarked = 1
                      AND (hidden = 0 OR hidden IS NULL)
                    ORDER BY date DESC
                    LIMIT ?
                """, (channel_id, info["title"], info["username"], limit * 3))
                for row in cursor.fetchall():
                    raw_messages.append(dict(row))
            except (sqlite3.Error, ValueError):
                pass

        return self._group_album_messages(raw_messages, limit, reverse=True)

    def get_channel_stats(self, channel_id: int) -> dict:
        """Get message statistics for a channel (excludes hidden messages)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN (read = 0 OR read IS NULL) AND (hidden = 0 OR hidden IS NULL) THEN 1 ELSE 0 END) as unread,
                    SUM(CASE WHEN bookmarked = 1 AND (hidden = 0 OR hidden IS NULL) THEN 1 ELSE 0 END) as bookmarked,
                    SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) as likes,
                    SUM(CASE WHEN rating = -1 THEN 1 ELSE 0 END) as dislikes
                FROM {table_name}
            """)
            row = cursor.fetchone()
            if row:
                return {
                    "total": row[0] or 0,
                    "unread": row[1] or 0,
                    "bookmarked": row[2] or 0,
                    "likes": row[3] or 0,
                    "dislikes": row[4] or 0
                }
        except sqlite3.Error:
            pass
        return {"total": 0, "unread": 0, "bookmarked": 0, "likes": 0, "dislikes": 0}

    def count_unread_by_group(self, exclusions: list[dict] | None = None) -> dict[int, int]:
        """Count unread messages per group using the exact same pipeline as message loading.

        Runs get_unread_messages_by_group (which includes album grouping) then
        applies tag exclusion filtering — identical to what the feed shows.
        """
        cursor = self.cursor()
        cursor.execute("SELECT id FROM groups")
        group_ids = [r[0] for r in cursor.fetchall()]

        counts: dict[int, int] = {}
        for group_id in group_ids:
            messages = self.get_unread_messages_by_group(group_id, limit=10000)
            if exclusions:
                messages = [m for m in messages
                            if not self.check_tag_exclusions(m.get('ai_summary') or '', exclusions)]
            counts[group_id] = len(messages)

        return counts

    # Read sync methods

    def mark_messages_read_up_to(self, channel_id: int, max_id: int) -> int:
        """Mark all messages up to max_id as read (syncing from Telegram)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                UPDATE {table_name} SET read = 1 WHERE id <= ? AND read = 0
            """, (max_id,))
            return cursor.rowcount
        except sqlite3.Error:
            return 0

    def get_unsynced_read_messages(self, channel_id: int, limit: int = 100) -> list[dict]:
        """Get messages that are read locally but not synced to Telegram."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id FROM {table_name}
                WHERE read = 1 AND (read_in_tg = 0 OR read_in_tg IS NULL)
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))
            return [{"id": row[0]} for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def mark_messages_synced_to_tg(self, channel_id: int, max_id: int) -> int:
        """Mark all messages up to max_id as synced to Telegram."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                UPDATE {table_name} SET read_in_tg = 1
                WHERE id <= ? AND read = 1 AND (read_in_tg = 0 OR read_in_tg IS NULL)
            """, (max_id,))
            return cursor.rowcount
        except sqlite3.Error:
            return 0

    # Content deduplication methods

    def get_messages_needing_hashes(self, channel_id: int, limit: int = 100, min_length: int = 50) -> list[dict]:
        """Get unread messages that need content hash generation."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, message, media_type, date
                FROM {table_name}
                WHERE (content_hash_pending = 1 OR content_hash_pending IS NULL)
                  AND (read = 0 OR read IS NULL)
                  AND message IS NOT NULL
                  AND length(message) >= ?
                ORDER BY date DESC
                LIMIT ?
            """, (min_length, limit))
            return [{"id": row[0], "message": row[1], "media_type": row[2], "date": row[3]}
                    for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def update_content_hash(self, channel_id: int, message_id: int, content_hash: str,
                            ai_summary: str | None = None) -> None:
        """Update content hash and AI summary for a message and mark as processed."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            UPDATE {table_name}
            SET content_hash = ?, ai_summary = ?, content_hash_pending = 0
            WHERE id = ?
        """, (content_hash, ai_summary, message_id))

    def skip_content_hash(self, channel_id: int, message_id: int) -> None:
        """Mark a message as skipped for hashing (too short, media-only, error, etc.)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            UPDATE {table_name} SET content_hash_pending = -1 WHERE id = ?
        """, (message_id,))

    def mark_as_duplicate(self, channel_id: int, message_id: int,
                          original_channel: int, original_message: int) -> None:
        """Mark a message as a duplicate of another message."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            UPDATE {table_name}
            SET duplicate_of_channel = ?, duplicate_of_message = ?
            WHERE id = ?
        """, (original_channel, original_message, message_id))

    def register_content_hash(self, content_hash: str, channel_id: int,
                              message_id: int, message_date: int,
                              group_id: int = 0) -> tuple[int, int] | None:
        """Register a content hash or return existing original if duplicate.

        Only matches within the same group_id.
        """
        cursor = self.cursor()
        cursor.execute("""
            SELECT channel_id, message_id FROM content_hashes
            WHERE hash = ? AND group_id = ?
        """, (content_hash, group_id))
        existing = cursor.fetchone()

        if existing:
            return (existing[0], existing[1])

        cursor.execute("""
            INSERT INTO content_hashes (hash, group_id, channel_id, message_id, message_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (content_hash, group_id, channel_id, message_id, message_date, int(time.time())))

        return None

    def get_short_messages_for_skip(self, channel_id: int, limit: int = 500, min_length: int = 50) -> list[int]:
        """Get unread message IDs that are too short for hashing and should be skipped."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id FROM {table_name}
                WHERE (content_hash_pending = 1 OR content_hash_pending IS NULL)
                  AND (read = 0 OR read IS NULL)
                  AND (message IS NULL OR length(message) < ?)
                LIMIT ?
            """, (min_length, limit))
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    # Media deduplication methods

    def get_messages_needing_media_hashes(self, channel_id: int, limit: int = 100) -> list[dict]:
        """Get unread messages that need media hash generation.

        Returns messages that have media_path (downloaded media) and media_hash_pending=1.
        """
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, media_path, media_type, grouped_id, date
                FROM {table_name}
                WHERE (media_hash_pending = 1 OR media_hash_pending IS NULL)
                  AND (read = 0 OR read IS NULL)
                  AND media_path IS NOT NULL
                ORDER BY date DESC
                LIMIT ?
            """, (limit,))
            return [{"id": row[0], "media_path": row[1], "media_type": row[2],
                     "grouped_id": row[3], "date": row[4]}
                    for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def get_album_messages(self, channel_id: int, grouped_id: int) -> list[dict]:
        """Get all messages in an album (same grouped_id)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT *
                FROM {table_name}
                WHERE grouped_id = ?
                ORDER BY id
            """, (grouped_id,))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def update_media_hash(self, channel_id: int, message_id: int, media_hash: str) -> None:
        """Update media hash for a message and mark as processed."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            UPDATE {table_name}
            SET media_hash = ?, media_hash_pending = 0
            WHERE id = ?
        """, (media_hash, message_id))

    def skip_media_hash(self, channel_id: int, message_id: int) -> None:
        """Mark a message as skipped for media hashing (no media, error, etc.)."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            UPDATE {table_name} SET media_hash_pending = -1 WHERE id = ?
        """, (message_id,))

    def register_media_hash(self, media_hash: str, channel_id: int,
                            message_id: int, message_date: int,
                            group_id: int = 0) -> tuple[int, int] | None:
        """Register a media hash or return existing original if duplicate.

        Only matches within the same group_id.
        """
        cursor = self.cursor()
        cursor.execute("""
            SELECT channel_id, message_id FROM media_hashes
            WHERE hash = ? AND group_id = ?
        """, (media_hash, group_id))
        existing = cursor.fetchone()

        if existing:
            return (existing[0], existing[1])

        cursor.execute("""
            INSERT INTO media_hashes (hash, group_id, channel_id, message_id, message_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (media_hash, group_id, channel_id, message_id, message_date, int(time.time())))

        return None

    def get_messages_without_media_for_skip(self, channel_id: int, limit: int = 500) -> list[int]:
        """Get unread message IDs that have no media and should be skipped for media hashing."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id FROM {table_name}
                WHERE (media_hash_pending = 1 OR media_hash_pending IS NULL)
                  AND (read = 0 OR read IS NULL)
                  AND media_path IS NULL
                LIMIT ?
            """, (limit,))
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    # Tag exclusion methods

    def get_all_tag_exclusions(self) -> list[dict]:
        """Get all tag exclusion groups."""
        cursor = self.cursor()
        cursor.execute("SELECT * FROM tag_exclusions ORDER BY created_at DESC")
        return [dict(row) for row in cursor.fetchall()]

    def create_tag_exclusion(self, tags: str) -> int:
        """Create a tag exclusion group. Normalizes tags (sort, dedupe, lowercase).
        Returns the id of the created/existing row."""
        normalized = ', '.join(sorted(set(
            t.strip().lower() for t in tags.split(',') if t.strip()
        )))
        if not normalized:
            return 0
        cursor = self.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO tag_exclusions (tags, created_at) VALUES (?, ?)",
            (normalized, int(time.time()))
        )
        if cursor.lastrowid:
            return cursor.lastrowid
        # Already exists, fetch id
        cursor.execute("SELECT id FROM tag_exclusions WHERE tags = ?", (normalized,))
        row = cursor.fetchone()
        return row[0] if row else 0

    def delete_tag_exclusion(self, exclusion_id: int) -> None:
        """Delete a tag exclusion group by id."""
        cursor = self.cursor()
        cursor.execute("DELETE FROM tag_exclusions WHERE id = ?", (exclusion_id,))

    @staticmethod
    def check_tag_exclusions(ai_summary: str, exclusions: list[dict]) -> bool:
        """Return True if ai_summary matches ANY exclusion group (all tags present)."""
        if not ai_summary:
            return False
        msg_tags = {t.strip().lower() for t in ai_summary.split(',') if t.strip()}
        for exc in exclusions:
            exc_tags = {t.strip().lower() for t in exc['tags'].split(',') if t.strip()}
            if exc_tags and exc_tags.issubset(msg_tags):
                return True
        return False

    # Telegram credentials methods (for tg_daemon)

    def get_all_tg_creds(self) -> list[dict]:
        """Get all Telegram credentials."""
        cursor = self.cursor()
        cursor.execute('SELECT id, api_id, api_hash, phone_number, "primary" FROM tg_creds')
        return [{"id": r[0], "api_id": r[1], "api_hash": r[2],
                 "phone_number": r[3], "primary": bool(r[4])} for r in cursor.fetchall()]

    def get_primary_tg_cred(self) -> dict | None:
        """Get the primary Telegram credential."""
        cursor = self.cursor()
        cursor.execute('SELECT id, api_id, api_hash, phone_number FROM tg_creds WHERE "primary" = 1 LIMIT 1')
        row = cursor.fetchone()
        if row:
            return {"id": row[0], "api_id": row[1], "api_hash": row[2], "phone_number": row[3], "primary": True}
        return None

    def get_tg_cred(self, cred_id: int) -> dict | None:
        """Get a specific Telegram credential by ID."""
        cursor = self.cursor()
        cursor.execute('SELECT id, api_id, api_hash, phone_number, "primary" FROM tg_creds WHERE id = ?', (cred_id,))
        row = cursor.fetchone()
        if row:
            return {"id": row[0], "api_id": row[1], "api_hash": row[2],
                    "phone_number": row[3], "primary": bool(row[4])}
        return None

    def add_tg_cred(self, api_id: int, api_hash: str, phone_number: str, primary: bool = False) -> int:
        """Add a new Telegram credential. Returns the new ID."""
        cursor = self.cursor()
        # If setting as primary, clear existing primary
        if primary:
            cursor.execute('UPDATE tg_creds SET "primary" = 0 WHERE "primary" = 1')
        cursor.execute("""
            INSERT INTO tg_creds (api_id, api_hash, phone_number, "primary")
            VALUES (?, ?, ?, ?)
        """, (api_id, api_hash, phone_number, 1 if primary else 0))
        return cursor.lastrowid

    def update_tg_cred(self, cred_id: int, api_id: int = None, api_hash: str = None,
                       phone_number: str = None, primary: bool = None) -> None:
        """Update a Telegram credential."""
        cursor = self.cursor()
        updates = []
        params = []
        if api_id is not None:
            updates.append("api_id = ?")
            params.append(api_id)
        if api_hash is not None:
            updates.append("api_hash = ?")
            params.append(api_hash)
        if phone_number is not None:
            updates.append("phone_number = ?")
            params.append(phone_number)
        if primary is not None:
            if primary:
                # Clear existing primary first
                cursor.execute('UPDATE tg_creds SET "primary" = 0 WHERE "primary" = 1')
            updates.append('"primary" = ?')
            params.append(1 if primary else 0)
        if updates:
            params.append(cred_id)
            cursor.execute(f"UPDATE tg_creds SET {', '.join(updates)} WHERE id = ?", params)

    def delete_tg_cred(self, cred_id: int) -> None:
        """Delete a Telegram credential."""
        cursor = self.cursor()
        cursor.execute("DELETE FROM tg_creds WHERE id = ?", (cred_id,))

    def set_primary_tg_cred(self, cred_id: int) -> None:
        """Set a credential as primary (clears other primaries)."""
        cursor = self.cursor()
        cursor.execute('UPDATE tg_creds SET "primary" = 0 WHERE "primary" = 1')
        cursor.execute('UPDATE tg_creds SET "primary" = 1 WHERE id = ?', (cred_id,))

    # Full-text search methods

    def search_messages(self, query: str, limit: int = 50, channel_id: int | None = None,
                        group_id: int | None = None) -> list[dict]:
        """Search messages using FTS5 trigram index (contentless mode).

        Args:
            query: Search query (min 3 characters for trigram matching)
            limit: Maximum number of results
            channel_id: Optional channel filter
            group_id: Optional group filter (searches all channels in group)

        Returns:
            List of dicts with channel_id, message_id, channel_title.
            Note: No snippet available in contentless mode - fetch full message separately.
        """
        if not query or len(query) < 3:
            return []

        cursor = self.cursor()

        # Get channel info for results
        cursor.execute("SELECT id, title, username FROM channels")
        channel_info = {row[0]: {"title": row[1], "username": row[2]} for row in cursor.fetchall()}

        # Get channel IDs to filter by
        allowed_channels = None
        if group_id is not None:
            channels = self.get_channels_by_group(group_id)
            allowed_channels = {c["id"] for c in channels}
        elif channel_id is not None:
            allowed_channels = {channel_id}

        # For trigram tokenizer, wrap query in double quotes for substring matching
        # Escape any double quotes in the query by doubling them
        escaped_query = query.replace('"', '""')
        safe_query = f'"{escaped_query}"'

        logger.info(f"[search_messages] Starting search for: '{query}'")
        logger.info(f"[search_messages] allowed_channels: {allowed_channels}")

        try:
            if allowed_channels:
                placeholders = ",".join("?" * len(allowed_channels))
                sql = f"""
                    SELECT channel_id, message_id
                    FROM messages_fts
                    WHERE messages_fts MATCH ?
                    AND channel_id IN ({placeholders})
                    LIMIT ?
                """
                params = (safe_query, *allowed_channels, limit)
                logger.info(f"[search_messages] SQL: {sql}")
                logger.info(f"[search_messages] Params: {params}")
                cursor.execute(sql, params)
            else:
                sql = """
                    SELECT channel_id, message_id
                    FROM messages_fts
                    WHERE messages_fts MATCH ?
                    LIMIT ?
                """
                params = (safe_query, limit)
                logger.info(f"[search_messages] SQL: {sql}")
                logger.info(f"[search_messages] Params: {params}")
                cursor.execute(sql, params)

            rows = cursor.fetchall()
            logger.info(f"[search_messages] Query returned {len(rows)} rows")

            results = []
            for row in rows:
                ch_id = row[0]
                info = channel_info.get(ch_id, {"title": "Unknown", "username": None})
                results.append({
                    "channel_id": ch_id,
                    "message_id": row[1],
                    "channel_title": info["title"],
                    "channel_username": info["username"]
                })
            logger.info(f"[search_messages] Returning {len(results)} results")
            return results
        except sqlite3.Error as e:
            logger.error(f"[search_messages] Search error: {e}", exc_info=True)
            return []

    def get_all_messages_for_indexing(self, channel_id: int) -> list[dict]:
        """Get all messages with text content for search indexing."""
        table_name = f"channel_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT id, message FROM {table_name}
                WHERE message IS NOT NULL
                AND length(message) >= 3
                ORDER BY id
            """)
            return [{"id": row[0], "message": row[1]} for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    def get_indexed_message_ids(self, channel_id: int) -> set[int]:
        """Get all message IDs that are already in the FTS index for a channel."""
        cursor = self.cursor()
        try:
            cursor.execute("""
                SELECT message_id FROM messages_fts WHERE channel_id = ?
            """, (channel_id,))
            return {row[0] for row in cursor.fetchall()}
        except sqlite3.Error:
            return set()

    def index_message_for_search(self, channel_id: int, message_id: int, message: str) -> bool:
        """Add a message to the FTS5 contentless search index.

        Returns True if indexed successfully.
        Note: In contentless mode, we just insert - duplicates will error.
        """
        cursor = self.cursor()
        try:
            cursor.execute("""
                INSERT INTO messages_fts(channel_id, message_id, message)
                VALUES (?, ?, ?)
            """, (channel_id, message_id, message))
            return True
        except sqlite3.Error as e:
            # IntegrityError or similar if duplicate - that's OK
            if "UNIQUE" in str(e).upper() or "constraint" in str(e).lower():
                return True
            logger.warning(f"Failed to index message {channel_id}/{message_id}: {e}")
            return False

    def index_messages_batch(self, channel_id: int, messages: list[dict]) -> int:
        """Batch index messages into FTS5.

        Args:
            channel_id: Channel ID
            messages: List of dicts with 'id' and 'message' keys

        Returns:
            Number of messages indexed
        """
        if not messages:
            return 0
        cursor = self.cursor()
        count = 0
        for msg in messages:
            try:
                cursor.execute("""
                    INSERT INTO messages_fts(channel_id, message_id, message)
                    VALUES (?, ?, ?)
                """, (channel_id, msg["id"], msg["message"]))
                count += 1
            except sqlite3.Error:
                pass  # Skip duplicates or errors
        return count

    def get_search_index_stats(self) -> dict:
        """Get statistics about the search index."""
        cursor = self.cursor()
        stats = {"indexed_messages": 0, "table_exists": False}
        try:
            # Check if table exists and get schema
            cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'"
            )
            row = cursor.fetchone()
            if row:
                stats["table_exists"] = True
                stats["is_contentless"] = "content=''" in row[0].lower()

            cursor.execute("SELECT COUNT(*) FROM messages_fts")
            stats["indexed_messages"] = cursor.fetchone()[0]

            # Get sample of indexed channels
            cursor.execute("""
                SELECT DISTINCT channel_id FROM messages_fts LIMIT 5
            """)
            stats["sample_channels"] = [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            stats["error"] = str(e)
        return stats

    def delete_from_search_index(self, channel_id: int, message_id: int, message: str) -> None:
        """Remove a message from the FTS5 contentless search index.

        Note: Contentless FTS5 requires providing the original content for deletion.
        """
        cursor = self.cursor()
        try:
            cursor.execute("""
                INSERT INTO messages_fts(messages_fts, channel_id, message_id, message)
                VALUES('delete', ?, ?, ?)
            """, (channel_id, message_id, message))
        except sqlite3.Error:
            pass

    def clear_search_index(self) -> None:
        """Clear the entire FTS5 search index (for rebuild)."""
        cursor = self.cursor()
        try:
            # Drop and recreate the FTS table
            cursor.execute("DROP TABLE IF EXISTS messages_fts")
            cursor.execute("""
                CREATE VIRTUAL TABLE messages_fts USING fts5(
                    channel_id UNINDEXED,
                    message_id UNINDEXED,
                    message,
                    tokenize="trigram"
                )
            """)
            logger.info("Cleared and recreated FTS5 search index")
        except sqlite3.Error as e:
            logger.warning(f"Failed to clear search index: {e}")

    def optimize_search_index(self) -> None:
        """Optimize the FTS5 index by merging b-trees."""
        cursor = self.cursor()
        try:
            cursor.execute("INSERT INTO messages_fts(messages_fts) VALUES('optimize')")
            logger.info("Search index optimized")
        except sqlite3.Error as e:
            logger.warning(f"Failed to optimize search index: {e}")

    # Backup hash methods for media file matching

    def create_backup_hash_table(self, channel_id: int) -> None:
        """Create a backup hash table for a channel if it doesn't exist."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                file_path TEXT PRIMARY KEY,
                file_size INTEGER NOT NULL,
                hash TEXT
            )
        """)
        # Index on hash for lookups
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_hash ON {table_name} (hash)
        """)
        # Index on file_size for small file lookups
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_size ON {table_name} (file_size)
        """)

    def get_backup_hash_count(self, channel_id: int) -> int:
        """Get the number of hashed files for a channel."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]
        except sqlite3.Error:
            return 0

    def insert_backup_hash(self, channel_id: int, file_path: str,
                           file_size: int, file_hash: str | None) -> None:
        """Insert or update a backup file hash."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name} (file_path, file_size, hash)
            VALUES (?, ?, ?)
        """, (file_path, file_size, file_hash))

    def insert_backup_hashes_batch(self, channel_id: int,
                                    hashes: list[tuple[str, int, str | None]]) -> int:
        """Batch insert backup file hashes. Each tuple is (file_path, file_size, hash).
        Returns count inserted."""
        if not hashes:
            return 0
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        cursor.executemany(f"""
            INSERT OR REPLACE INTO {table_name} (file_path, file_size, hash)
            VALUES (?, ?, ?)
        """, hashes)
        return cursor.rowcount

    def find_backup_by_hash(self, channel_id: int, file_hash: str) -> str | None:
        """Find a backup file by its hash. Returns file path or None."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"""
                SELECT file_path FROM {table_name} WHERE hash = ? LIMIT 1
            """, (file_hash,))
            row = cursor.fetchone()
            return row[0] if row else None
        except sqlite3.Error:
            return None

    def get_existing_backup_paths(self, channel_id: int) -> set[str]:
        """Get all file paths already in the backup hash table."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"SELECT file_path FROM {table_name}")
            return {row[0] for row in cursor.fetchall()}
        except sqlite3.Error:
            return set()

    def clear_backup_hashes(self, channel_id: int) -> None:
        """Clear all backup hashes for a channel."""
        table_name = f"channel_backup_hash_{channel_id}"
        cursor = self.cursor()
        try:
            cursor.execute(f"DELETE FROM {table_name}")
        except sqlite3.Error:
            pass
