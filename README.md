# TGFeed

A self-hosted Telegram channel aggregator with a web UI. Download messages from your subscribed Telegram channels and browse them locally with features like bookmarks, ratings, and read tracking.

## Features

- **Channel Sync**: Download messages from your Telegram channels
- **Web Interface**: Browse messages in a clean, responsive UI
- **Groups**: Organize channels into custom groups
- **Read Tracking**: Automatically track read/unread messages
- **Bookmarks**: Save messages for later
- **Ratings**: Like/dislike messages
- **Media Support**: Download and view photos, videos, audio, documents
- **Telegraph Integration**: Download and embed telegra.ph articles
- **History Download**: Optionally download full channel history
- **Debug Mode**: Press Ctrl+D to view message IDs and metadata

## Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) - Python package manager
- Telegram API credentials

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd tgfeed
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Get Telegram API credentials**
   - Go to https://my.telegram.org/apps
   - Create a new application
   - Note your `api_id` and `api_hash`

4. **Configure the application**

   Create a `.env` file in the project root:
   ```env
   API_ID=your_api_id
   API_HASH=your_api_hash
   PHONE_NUMBER=+1234567890
   ```

5. **Create data directories**
   ```bash
   mkdir -p data/media data/photos data/telegraph/css data/sessions
   ```

## Usage

### Starting the Daemon

The daemon maintains a persistent Telegram connection and handles all API operations.

**Windows:**
```batch
daemon.bat
```

**Linux/macOS:**
```bash
chmod +x daemon.sh
./daemon.sh
```

On first run, you'll need to authenticate with Telegram (enter the code sent to your phone).

### Syncing Messages

Run the sync loop to continuously download new messages:

**Windows:**
```batch
sync.bat
```

**Linux/macOS:**
```bash
chmod +x sync.sh
./sync.sh
```

This runs in a loop and:
1. Syncs read status back to Telegram
2. Downloads channel list updates
3. Downloads new messages from active channels
4. Downloads telegra.ph pages

### Starting the Web Server

**Windows:**
```batch
web.bat
```

**Linux/macOS:**
```bash
chmod +x web.sh
./web.sh
```

Then open http://localhost:8910 in your browser.

### Background Services

Run cleanup and thumbnail generation in the background:

**Windows:**
```batch
sync_service.bat
```

**Linux/macOS:**
```bash
chmod +x sync_service.sh
./sync_service.sh
```

## Web UI Guide

### Navigation

- **Group Tabs**: Click to filter messages by channel group
- **Unread Badges**: Numbers on tabs show unread message count
- **Bookmark Tab**: Click the bookmark icon to view saved messages
- **Channel Filter**: Click a channel name in a message to filter to that channel only

### Sidebar (Burger Menu)

Click the hamburger menu (☰) to open the sidebar:

- **Active Toggle**: Enable/disable message downloading for a channel
- **All Checkbox**: Enable full history download (downloads entire channel history)
- **Group Dropdown**: Assign channel to a group
- **Stats**: View unread count, bookmarks, likes/dislikes per channel

### Message Interactions

- **Skip Button (↓)**: Floating button on the left - click to scroll to next message
- **Like/Dislike**: Rate messages with thumbs up/down
- **Bookmark**: Save messages for later viewing
- **Read Tracking**: Messages are automatically marked as read when scrolled past

### Media

- **Images**: Click to view full size
- **Videos**: Play inline with controls
- **Audio**: Play with built-in player
- **Documents**: Download links with file type icons
- **Albums**: Multiple images/videos grouped together

### Embedded Content

- **YouTube**: Videos are embedded as players
- **Telegraph**: Articles show as expandable previews

### Debug Mode

Press **Ctrl+D** to toggle debug mode, which displays:
- Message ID and Channel ID
- Timestamps (date, edit_date, created_at)
- Engagement stats (views, forwards, replies)
- Media info (type, path, pending status)
- Entity data (formatting/links JSON)

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `daemon.sh/.bat` | Run Telegram daemon (must be running for sync) |
| `sync.sh/.bat` | Main sync loop - downloads new messages |
| `sync_service.sh/.bat` | Background tasks - cleanup, thumbnails, hashes |
| `web.sh/.bat` | Start web server |
| `sync_channels.py` | Download channel list (run manually if needed) |
| `sync_messages.py` | Download messages (run manually if needed) |
| `download_telegraph.py` | Download telegra.ph pages |
| `cleanup.py` | Remove old messages from non-archived channels |
| `fix_media_paths.py` | Fix misplaced media files (use --dry-run first) |

## Configuration Options

### Environment Variables (.env)

| Variable | Description | Default |
|----------|-------------|---------|
| `API_ID` | Telegram API ID | Required |
| `API_HASH` | Telegram API Hash | Required |
| `PHONE_NUMBER` | Your phone number | Required |
| `TG_DAEMON_HOST` | Daemon host | `127.0.0.1` |
| `TG_DAEMON_PORT` | Daemon port | `9876` |
| `MISTRAL_API_KEY` | API key for content deduplication | Optional |
| `MISTRAL_MODEL` | Mistral model name | `mistral-small-latest` |
| `DEDUP_MIN_MESSAGE_LENGTH` | Min message length for dedup | `50` |
| `DEDUP_MESSAGES_PER_RUN` | Messages to process per run | `100` |

### Channel Settings (via Web UI)

- **Active**: Whether to download messages from this channel
- **Group**: Organize channels into groups for filtered viewing
- **Download All**: Download complete channel history (backward sync)

## Data Storage

All data is stored in the `data/` directory:

```
data/
├── tgfeed.db          # SQLite database
├── sessions/          # Telegram session files
├── photos/            # Channel profile photos
├── media/             # Downloaded message media
│   └── {channel_id}/  # Organized by channel
└── telegraph/         # Downloaded telegra.ph pages
    ├── css/           # Shared CSS files
    └── {channel_id}/  # HTML files per channel
```

## Troubleshooting

### "Daemon not running" error
Make sure `daemon.sh` or `daemon.bat` is running before starting sync.

### Media not loading (404 errors)
1. Check that the file exists in `data/media/{channel_id}/`
2. Restart the web server
3. Run `fix_media_paths.py --dry-run` to check for misplaced files

### Messages not syncing
1. Ensure the channel is marked as "Active" in the sidebar
2. Check that the daemon is running
3. Look for errors in the daemon console output

### Authentication issues
Delete `data/sessions/` and restart the daemon to re-authenticate.

## License

MIT
