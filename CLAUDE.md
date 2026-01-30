# TGFeed - Telegram Channel Aggregator

A Python application to download and view Telegram channel messages locally with a web UI.

## Project Structure

```
tgfeed/
├── config.py              # Configuration from .env (API keys, paths, daemon settings)
├── database.py            # SQLite database operations and migrations
├── tg_daemon.py           # Centralized Telegram connection daemon (RPC server)
├── tg_client.py           # RPC client for communicating with tg_daemon
├── sync_channels.py       # Downloads channel list from Telegram
├── sync_messages.py       # Downloads messages from active channels
├── sync_history.py        # Downloads historical messages (backward sync)
├── sync_read_to_tg.py     # Syncs read status from TGFeed to Telegram
├── download_telegraph.py  # Downloads telegra.ph pages with embedded images
├── generate_thumbnails.py # Creates video thumbnails (2x2 grid) using ffmpeg
├── generate_content_hashes.py # LLM-based content deduplication via Claude API
├── index_search.py        # Indexes messages for full-text search (FTS5)
├── cleanup.py             # Removes old messages from non-archived channels
├── orchestrator.py        # TUI for managing all scripts (start/stop/logs)
├── web.py                 # Bottle web server for UI
├── templates/
│   ├── index.html         # Single-page web application
│   └── static/            # Favicon files
├── data/                  # Runtime data (configured in .env)
│   ├── tgfeed.db          # SQLite database
│   ├── session.session    # Legacy Telethon session file
│   ├── sessions/          # Per-credential session files (for daemon)
│   ├── photos/            # Channel profile photos ({channel_id}.jpg)
│   ├── media/             # Downloaded message media ({channel_id}/...)
│   ├── logs/              # Error logs from orchestrator ({script}_{timestamp}.log)
│   └── telegraph/         # Downloaded telegra.ph pages
│       ├── css/           # Deduplicated CSS files (content-hashed)
│       └── {channel_id}/  # HTML files per channel
├── sync.bat / sync.sh     # Loop: sync_read_to_tg + sync_channels + sync_messages + download_telegraph
├── sync_service.bat / sync_service.sh  # Runs maintenance tasks (cleanup, thumbnails, content hashes)
├── web.bat / web.sh       # Starts web server
├── daemon.bat / daemon.sh # Starts Telegram daemon
└── orchestrator.bat / orchestrator.sh  # Starts orchestrator TUI (auto-starts everything)
```

## Database Schema

### `channels` table
Stores Telegram channel metadata:
- `id` (PRIMARY KEY) - Telegram channel ID
- `access_hash` - Required for Telegram API calls
- `title`, `username` - Channel display info
- `photo_id` - ID of channel photo
- `date` - Channel creation date
- `participants_count` - Number of subscribers
- `broadcast`, `megagroup` - Channel type flags
- `verified`, `restricted`, `scam`, `fake` - Channel status flags
- `subscribed` - 1 if channel exists in user's Telegram
- `active` - 1 if messages should be downloaded (user toggle)
- `group_id` - FK to groups table for organizing channels
- `download_all` - 1 to download full history (backward sync)
- `last_active` - Timestamp of last new message download
- `backup_path` - Path to Telegram Desktop backup folder (for media recovery)
- `download_images` - 1 to download images for new messages (when not download_all)
- `download_videos` - 1 to download videos for new messages (when not download_all)
- `download_audio` - 1 to download audio/voice for new messages (when not download_all)
- `download_other` - 1 to download other files for new messages (when not download_all)

### `groups` table
User-defined groups for organizing channels:
- `id` (INTEGER PRIMARY KEY)
- `name` (TEXT)

### `tg_creds` table
Telegram API credentials for daemon (supports multiple accounts):
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `api_id` (INTEGER) - Telegram API ID
- `api_hash` (TEXT) - Telegram API hash
- `phone_number` (TEXT) - Phone number
- `primary` (INTEGER) - 1 if this is the primary account

### `content_hashes` table
For LLM-based content deduplication:
- `hash` (TEXT PRIMARY KEY) - Hash of LLM-generated summary
- `channel_id` (INTEGER)
- `message_id` (INTEGER)
- `message_date` (INTEGER)
- `created_at` (INTEGER)

### `messages_fts` table (FTS5 virtual table)
Full-text search index using SQLite FTS5 with trigram tokenizer:
- `channel_id` (UNINDEXED) - Channel ID for filtering
- `message_id` (UNINDEXED) - Message ID within channel
- `message` - Indexed message text (trigram tokenized)

The trigram tokenizer enables substring matching (e.g., searching "gram" finds "telegram"). Minimum query length is 3 characters. Search returns channel_id/message_id pairs, and full message data is fetched from channel_* tables.

### `channel_backup_hash_{id}` tables (dynamic)
Per-channel backup file index for media recovery:
- `file_path` (TEXT PRIMARY KEY) - Full path to backup file
- `file_size` (INTEGER) - Size in bytes
- `file_hash` (TEXT) - MD5 hash of first 64KB (NULL for files ≤64KB)

### `channel_{id}` tables (dynamic)
One table per channel for messages. Created on first sync.

**Core fields:**
- `id` (PRIMARY KEY) - Telegram message ID
- `date` - Unix timestamp
- `message` - Text content
- `entities` - JSON of formatting/links (MessageEntityBold, MessageEntityUrl, etc.)

**Metadata:**
- `out`, `mentioned`, `media_unread`, `silent`, `post` - Message flags (0/1)
- `from_id` - Sender ID (for groups)
- `fwd_from_id`, `fwd_from_name` - Forwarded message info
- `reply_to_msg_id` - ID of message being replied to
- `views`, `forwards`, `replies` - Engagement counts
- `edit_date` - Last edit timestamp
- `post_author` - Author name for signed posts
- `grouped_id` - Links album messages together

**Media:**
- `media_type` - photo, video, audio, voice, document, sticker, animation, webpage, poll, etc.
- `media_path` - Relative path in data/media/
- `media_pending` - 1 if media download failed and should be retried
- `video_thumbnail_path` - Path to generated video thumbnail

**User state:**
- `read` - 1 if user has scrolled past this message
- `read_synced_to_tg` - 1 if read status was synced back to Telegram
- `rating` - -1 (dislike), 0 (none), 1 (like)
- `bookmarked` - 1 if saved
- `html_downloaded` - 1 if telegra.ph page has been downloaded
- `created_at` - When message was added to database

**Deduplication:**
- `ai_summary` - LLM-generated summary of message content
- `content_hash` - Hash of the AI summary for duplicate detection
- `content_hash_pending` - 1 if needs processing, -1 if skipped, 0 if done
- `duplicate_of_channel` - Channel ID of original message (if duplicate)
- `duplicate_of_message` - Message ID of original message (if duplicate)

**Indexes:**
- `idx_channel_{id}_date` - For date ordering
- `idx_channel_{id}_read_date` - Composite for unread queries
- `idx_channel_{id}_bookmarked` - For bookmark queries
- `idx_channel_{id}_content_hash` - For duplicate detection

## Architecture

### Telegram Daemon (tg_daemon.py)
Centralized long-running process that manages Telegram connections:
- Exposes JSON-RPC over TCP (default: localhost:9876)
- Supports multiple Telegram accounts via `tg_creds` table
- Handles connection pooling, reconnection, flood wait errors
- All sync scripts can use daemon or fall back to direct connection

**RPC Methods:**
- `ping` - Health check
- `get_clients` - List connected accounts
- `iter_dialogs` - Get all dialogs (channels, chats)
- `iter_messages` - Fetch messages from a channel
- `get_messages` - Get specific messages by ID
- `download_media` - Download media file
- `download_profile_photo` - Download channel photo
- `get_media_hash` - Download first 64KB of media and return MD5 hash (for backup matching)
- `send_read_acknowledge` - Mark messages as read in Telegram

### RPC Client (tg_client.py)
Async client library for communicating with tg_daemon:
- Connection management with automatic reconnection
- Error handling (TGClientError, TGFloodWaitError)
- `is_daemon_running()` helper to check if daemon is available

## Sync Logic

### sync_channels.py
1. Uses tg_daemon if available, else direct Telethon connection
2. Iterates all dialogs, filters broadcast channels
3. Downloads channel profile photos to data/photos/
4. Upserts channels to database (preserves active/group/backup_path settings)
5. Marks removed channels as unsubscribed

### sync_messages.py

**Forward sync (new messages):**
1. For each channel with `active=1`:
   - Get `latest_id` from database
   - If no messages: download only the latest message (first sync)
   - Otherwise: download all messages newer than `latest_id`
   - Check channel media settings (`download_images`, `download_videos`, etc.)
   - For channels with `download_all=0`: only download media types with flag=1
   - For channels with `download_all=1`: download all media types
   - Media not downloaded due to settings: saved with `media_type` but no `media_path`
   - If media download fails: set `media_pending=1`
   - Update `last_active` timestamp if new messages found

2. After forward sync, retry pending media downloads (up to 5 per channel):
   - Query messages with `media_pending=1` and `media_path IS NULL`

**Media type mapping:**
- `photo` -> `download_images` flag
- `video` -> `download_videos` flag
- `audio`, `voice` -> `download_audio` flag
- Everything else (document, sticker, etc.) -> `download_other` flag
   - Re-fetch message from Telegram and attempt download
   - On success: update `media_path` and clear `media_pending`

**Media handling:**
- Uses Telethon's `download_media()` without timeout
- Supports DC migration (FileMigrateError) naturally
- Failed downloads tracked via `media_pending` column

### sync_history.py

**Backward sync (full history download):**
Runs in a continuous loop with 60-second pause between runs.

1. For each channel with `download_all=1`:
   - Get `oldest_id` from database
   - Download batch of messages older than `oldest_id`
   - Mark as `read=1` (historical messages)
   - Download media concurrently (configurable concurrency)
   - Repeats each sync run until reaching message ID 1

**Backup media recovery:**
If channel has `backup_path` set (pointing to Telegram Desktop export):
- Scans backup folder and indexes files by hash (MD5 of first 64KB)
- For large files (>64KB): uses `get_media_hash` RPC to download only first 64KB
- Computes hash and checks backup index for matching file
- If hash matches: copies from backup (no full download needed)
- If no match: downloads full file from Telegram
- Small files (≤64KB): downloaded directly without hash matching

### sync_read_to_tg.py
Syncs local read status back to Telegram:
1. Finds messages where `read=1` but `read_synced_to_tg=0`
2. Calls `send_read_acknowledge` with highest message ID
3. Updates `read_synced_to_tg=1` for synced messages

### download_telegraph.py
Downloads telegra.ph articles referenced in messages:
1. Scans messages for telegra.ph URLs in `entities` JSON and `message` text
2. Downloads HTML page
3. Embeds images as base64 data URIs
4. Downloads CSS files, deduplicates by content hash
5. Replaces CSS links with local paths (`/telegraph/css/{hash}.css`)
6. Saves HTML to `data/telegraph/{channel_id}/{slug}.html`
7. Sets `html_downloaded=1` only if all URLs succeeded

### generate_thumbnails.py
Creates video thumbnails using ffmpeg:
1. Queries videos without `video_thumbnail_path`
2. Generates 2x2 grid thumbnail (4 frames from video)
3. Saves as `{video_filename}_thumb.jpg` alongside video
4. Updates `video_thumbnail_path` in database

### generate_content_hashes.py
LLM-based content deduplication using Claude API:
1. Queries messages without content hash (min length configurable)
2. Sends message text to Claude for normalization/summarization
3. Hashes the normalized summary
4. Stores hash in message table and registers in `content_hashes` lookup table
5. If hash already exists, marks message as duplicate of the original

### index_search.py
Full-text search indexing using SQLite FTS5:
1. Creates FTS5 virtual table with trigram tokenizer
2. Iterates all active channels
3. Compares messages in channel tables vs FTS index
4. Indexes new messages (no tracking column needed - checks FTS directly)
5. Optional `--optimize` flag to merge FTS5 b-trees for better performance
6. Optional `--rebuild` flag to clear and rebuild entire index from scratch

Run periodically (e.g., in sync_service) to keep search index up to date.

### cleanup.py
Removes old messages to save disk space:
1. For channels without `download_all=1`:
   - Delete messages older than 30 days (always keeps at least one message - the most recent)
   - Delete associated media files
   - Remove entries from FTS search index
   - Remove empty media directories
2. Channels with `download_all=1` are preserved entirely

## Web UI Architecture

### Backend (web.py)
Bottle framework with Waitress server (multi-threaded for concurrent requests).

**API Endpoints:**
- `GET /api/channels` - All channels with group info and stats
- `GET /api/groups` - All groups
- `POST /api/group` - Create new group
- `PUT /api/group/{id}` - Update group name
- `DELETE /api/group/{id}` - Delete group
- `GET /api/group/{id}/messages?limit=N&channel=ID` - Unread messages for group
- `GET /api/group/{id}/earlier?before=TIMESTAMP&limit=N&channel=ID` - Earlier (read) messages
- `GET /api/channel/{id}/oldest?limit=N` - Oldest messages for a channel (for "jump to oldest")
- `GET /api/channel/{id}/later?after=TIMESTAMP&limit=N` - Messages newer than timestamp (for scroll-down loading)
- `GET /api/message/{channel_id}/{message_id}` - Single message by ID
- `GET /api/bookmarks?limit=N` - All bookmarked messages (newest first)
- `GET /api/search?q=QUERY&limit=N&channel=ID&group=ID` - Full-text search (min 3 chars)
- `GET /api/search/stats` - Search index statistics
- `POST /api/channel/{id}/active` - Toggle message downloading
- `POST /api/channel/{id}/group` - Set channel's group
- `POST /api/channel/{id}/download_all` - Toggle history download
- `POST /api/channel/{id}/backup_path` - Set backup path for media recovery
- `POST /api/channel/{id}/media_settings` - Set media download flags (images, videos, audio, other)
- `GET /api/live-media/{channel_id}/{message_id}` - Stream media from Telegram on-demand (transparent, no save)
- `POST /api/messages/read` - Mark messages as read (batch)
- `POST /api/message/rate` - Set rating (-1, 0, 1)
- `POST /api/message/bookmark` - Toggle bookmark

**Static file routes:**
- `/media/{path}` - Message media (1 year cache, immutable)
- `/static/{path}` - UI assets (1 week cache)
- `/api/channel/{id}/photo` - Channel photos (1 day cache)
- `/telegraph/css/{file}` - Telegraph CSS (1 year cache, content-hashed)
- `/telegraph/{channel_id}/{slug}.html` - Telegraph HTML (1 day cache)

**Database optimizations:**
- WAL mode for concurrent reads/writes
- 64MB cache, busy timeout 10s
- Batch operations for marking messages read

### Frontend (index.html)
Single-page app with vanilla JavaScript. No build step.

**Layout:**
- Header: Group tabs with unread badges + bookmark button + search button
- Sidebar (burger menu): Channel list with toggles
- Main: Message feed with skip buttons

**Key Features:**

*Navigation:*
- Group tabs filter messages by channel group
- Unread counts displayed as badges on group tabs
- Click channel name in message to filter to that channel
- URL state preserved (`?group=ID&channel=ID&search=QUERY`)

*Search:*
- Click magnifying glass icon in header to expand search input
- Searches message text using FTS5 trigram index (substring matching)
- Minimum query length: 3 characters
- Results show matching messages with highlighted snippets
- Can search within current group or across all channels

*Channel management (sidebar):*
- Active toggle - enable/disable message downloading
- "All" checkbox - enable full history download
- Media type toggles (visible when active but not "All"):
  - Images, Videos, Audio, Other - select which media types to download
  - Unselected types can still be viewed via live-view on demand
- Backup path input - set local backup folder for media recovery
- Group dropdown - assign channel to group
- Stats: unread count, bookmarks, likes/dislikes

*Message display:*
- Message cards with channel icon, title, date, text, media
- Clickable timestamps link to original message on Telegram (t.me)
- Album support: messages with same `grouped_id` combined
- Reply quotes: fetches and displays replied-to message with full media
- Entity rendering: Bold, italic, links, code blocks
- Handles UTF-16 offsets for emoji in entity positions
- Duplicate indicator for content detected as similar to earlier messages

*Embedded content:*
- YouTube videos: detected from URLs, embedded as iframes
- Telegraph pages: preview iframe with expand/collapse, open in new tab

*Media:*
- Lazy loading for images
- Image lightbox for full-size viewing
- Videos with controls, `preload="metadata"`
- Video thumbnails (2x2 grid) with click to play
- Audio players with filename display
- File attachments with type icons
- Live-view: media not downloaded locally streams transparently from Telegram
  - No visual difference between local and streamed media
  - Streams via daemon without saving to disk

*User interactions:*
- Skip button (↓) - floating on left, scrolls to next message
- Jump to oldest button (↑) - appears when channel is filtered, jumps to first message
- Rating buttons (thumbs up/down)
- Bookmark button
- Scroll-based read tracking with IntersectionObserver

*Load more:*
- Scroll to top loads earlier (read) messages
- Scroll to bottom loads later messages (when viewing from oldest)
- Maintains scroll position when prepending/appending

*Debug mode (Ctrl+D):*
- Shows channel IDs next to channel names
- Shows message debug info: message ID, channel ID, timestamps, media info
- Shows AI summary and content hash if generated
- Shows duplicate detection info

**Media Type Detection:**
`getEffectiveMediaType()` checks file extension when `media_type` is "document" to properly display images/videos/audio that Telegram marks as documents.

## Configuration (.env)

```env
# Telegram API (required)
API_ID=12345678
API_HASH=your_api_hash_here
PHONE_NUMBER=+1234567890

# Telegram daemon (optional, defaults shown)
TG_DAEMON_HOST=127.0.0.1
TG_DAEMON_PORT=9876

# Web UI (optional, defaults shown)
WEB_HOST=0.0.0.0
WEB_PORT=8910

# Claude API for content deduplication (optional)
ANTHROPIC_API_KEY=your_anthropic_api_key
CLAUDE_MODEL=claude-haiku-4-5

# Deduplication settings (optional)
DEDUP_MIN_MESSAGE_LENGTH=50
DEDUP_MESSAGES_PER_RUN=100
```

Get API credentials from https://my.telegram.org/apps

## Running

**Windows:**
```batch
# Start the Telegram daemon (recommended, run in separate terminal)
daemon.bat

# Continuous sync loop
sync.bat

# Maintenance tasks (cleanup, thumbnails, content hashes)
sync_service.bat

# Start web UI
web.bat
# Open http://localhost:8910
```

**Linux/macOS:**
```bash
# Start the Telegram daemon (recommended, run in separate terminal)
./daemon.sh

# Continuous sync loop
./sync.sh

# Maintenance tasks (cleanup, thumbnails, content hashes)
./sync_service.sh

# Start web UI
./web.sh
# Open http://localhost:8910
```

**Orchestrator (TUI for managing all scripts):**
```bash
orchestrator.bat   # Windows
./orchestrator.sh  # Linux/macOS
```
The orchestrator provides a terminal UI to start/stop scripts, view their status and logs. **On launch, it automatically starts everything:** both daemons (Telegram + Web), both chains (sync + maintenance), and the history script. Uses file-based logging for stability (writes to temp files, reads for display). When a script fails (non-zero exit code), full logs are saved to `data/logs/{script}_{timestamp}.log`.

**Orchestrator controls:**
- `↑/↓` or `j/k` - Navigate scripts (auto-shows logs for selected script)
- `Enter` or `s` - Start selected script
- `x` - Stop selected script
- `l` - Toggle log view
- `a` - Start all daemons
- `F1` - Toggle sync chain (read-sync → channels → messages → telegraph, loops)
- `F2` - Toggle maintenance chain (thumbnails → hashes → search → cleanup, loops)
- `q` - Quit (stops all scripts)

**Script status indicators:**
- `Running` (green) - Script is currently executing
- `Completed` (cyan) - Script finished successfully (exit code 0)
- `Failed (N)` (red) - Script crashed with exit code N, log saved to file
- `Stopped` (dim) - Script not running

**Individual scripts (use `uv run python` for all scripts):**
```bash
uv run python sync_channels.py        # Sync channel list
uv run python sync_messages.py        # Sync new messages
uv run python sync_history.py         # Download historical messages
uv run python sync_read_to_tg.py      # Sync read status to Telegram
uv run python download_telegraph.py   # Download telegra.ph pages
uv run python generate_thumbnails.py  # Generate video thumbnails
uv run python generate_content_hashes.py  # Generate AI summaries for deduplication
uv run python index_search.py         # Index messages for full-text search
uv run python index_search.py --optimize  # Index and optimize FTS5 index
uv run python index_search.py --rebuild   # Rebuild entire search index from scratch
uv run python cleanup.py              # Clean up old messages
```

## Dependencies

- telethon - Telegram MTProto client
- bottle - Lightweight web framework
- waitress - Production WSGI server
- python-dotenv - Environment configuration
- requests - HTTP client (for telegraph downloads)
- rich - Terminal UI for orchestrator
- ffmpeg (system) - For video thumbnail generation (optional)
- Claude API - For content deduplication (optional, uses requests library)

## Database Migrations

Migrations run automatically on sync. The `DatabaseMigration` class:
1. Creates `channels` table if not exists
2. Creates `groups` table if not exists
3. Creates `tg_creds` table for daemon credentials
4. Creates `content_hashes` table for deduplication
5. Creates `messages_fts` FTS5 table for full-text search
6. Adds new columns to `channels` (`last_active`, `download_all`, `backup_path`)
7. Adds new columns to all `channel_*` tables:
   - `media_path`, `entities`, `read`, `rating`, `bookmarked`
   - `html_downloaded`, `media_pending`, `video_thumbnail_path`
   - `read_synced_to_tg`, `ai_summary`, `content_hash`, `content_hash_pending`
   - `duplicate_of_channel`, `duplicate_of_message`
8. Creates indexes for efficient queries

The `create_channel_messages_table()` method also handles migrations for individual channel tables, ensuring columns exist before creating indexes.

## Error Handling

**Media download failures:**
- Telegram may return `FileMigrateError` requiring DC switch
- No timeout used - downloads complete naturally
- Failed downloads tracked in `media_pending` column
- Retried on subsequent sync runs (up to 5 per channel)

**Telegraph download failures:**
- Individual URL failures logged but don't stop processing
- `html_downloaded` only set to 1 if ALL URLs succeeded
- Failed pages will be retried on next run

**Daemon connection failures:**
- All sync scripts check `is_daemon_running()` first
- If daemon unavailable, fall back to direct Telethon connection
- Graceful degradation - scripts work with or without daemon
