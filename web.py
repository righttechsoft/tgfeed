"""Web UI for TGFeed."""

import asyncio
import json
import logging
import time
from bottle import Bottle, request, response, static_file, TEMPLATE_PATH
from pathlib import Path

from config import DATA_DIR, MEDIA_DIR, WEB_HOST, WEB_PORT, PAUSE_FILE
from database import Database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Bottle()

# Templates directory
TEMPLATE_DIR = Path(__file__).parent / "templates"
TEMPLATE_DIR.mkdir(exist_ok=True)

# Static files directory
STATIC_DIR = TEMPLATE_DIR / "static"


@app.route("/")
def index():
    """Main page."""
    return TEMPLATE_DIR.joinpath("index.html").read_text(encoding="utf-8")


@app.route("/api/channels")
def get_channels():
    """Get all channels with groups and stats."""
    response.content_type = "application/json"
    with Database() as db:
        channels = db.get_all_channels_with_groups()
        result = []
        for row in channels:
            channel = dict(row)
            stats = db.get_channel_stats(channel["id"])
            channel["stats"] = stats
            result.append(channel)
        return json.dumps(result)


@app.route("/api/groups")
def get_groups():
    """Get all groups with channel counts."""
    response.content_type = "application/json"
    with Database() as db:
        groups = db.get_all_groups()
        result = []
        for row in groups:
            group = dict(row)
            group["channel_count"] = db.get_group_channel_count(group["id"])
            result.append(group)
        return json.dumps(result)


@app.route("/api/group", method="POST")
def create_group():
    """Create a new group."""
    response.content_type = "application/json"
    data = request.json
    name = data.get("name", "").strip()
    if not name:
        response.status = 400
        return json.dumps({"error": "Group name is required"})
    with Database() as db:
        group_id = db.create_group(name)
        db.commit()
        return json.dumps({"success": True, "id": group_id, "name": name})


@app.route("/api/group/<group_id:int>", method="PUT")
def rename_group(group_id):
    """Rename a group."""
    response.content_type = "application/json"
    data = request.json
    name = data.get("name", "").strip()
    if not name:
        response.status = 400
        return json.dumps({"error": "Group name is required"})
    with Database() as db:
        db.rename_group(group_id, name)
        db.commit()
        return json.dumps({"success": True})


@app.route("/api/group/<group_id:int>", method="DELETE")
def delete_group(group_id):
    """Delete a group."""
    response.content_type = "application/json"
    with Database() as db:
        db.delete_group(group_id)
        db.commit()
        return json.dumps({"success": True})


@app.route("/api/channel/<channel_id:int>/active", method="POST")
def update_active(channel_id):
    """Update channel active status."""
    response.content_type = "application/json"
    data = request.json
    active = 1 if data.get("active") else 0
    with Database() as db:
        db.update_channel_active(channel_id, active)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/channel/<channel_id:int>/group", method="POST")
def update_group(channel_id):
    """Update channel group."""
    response.content_type = "application/json"
    data = request.json
    group_id = data.get("group_id")
    if group_id == "":
        group_id = None
    elif group_id is not None:
        group_id = int(group_id)
    with Database() as db:
        db.update_channel_group(channel_id, group_id)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/channel/<channel_id:int>/download_all", method="POST")
def update_download_all(channel_id):
    """Update channel download_all status."""
    response.content_type = "application/json"
    data = request.json
    download_all = 1 if data.get("download_all") else 0
    with Database() as db:
        db.update_channel_download_all(channel_id, download_all)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/channel/<channel_id:int>/backup_path", method="POST")
def update_backup_path(channel_id):
    """Update channel backup_path for local media lookup."""
    response.content_type = "application/json"
    data = request.json
    backup_path = data.get("backup_path")
    # Allow empty string or None to clear the path
    if backup_path == "":
        backup_path = None
    with Database() as db:
        db.update_channel_backup_path(channel_id, backup_path)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/channel/<channel_id:int>/media_settings", method="POST")
def update_media_settings(channel_id):
    """Update channel media download settings."""
    response.content_type = "application/json"
    data = request.json
    images = 1 if data.get("download_images") else 0
    videos = 1 if data.get("download_videos") else 0
    audio = 1 if data.get("download_audio") else 0
    other = 1 if data.get("download_other") else 0
    with Database() as db:
        db.update_channel_media_settings(channel_id, images, videos, audio, other)
        db.commit()
    return json.dumps({"success": True})


@app.route("/media/<filepath:path>")
def serve_media(filepath):
    """Serve media files with cache headers."""
    media_dir = DATA_DIR / "media"
    full_path = media_dir / filepath
    if not full_path.exists():
        logger.warning(f"Media file not found: {full_path} (filepath={filepath}, media_dir={media_dir})")
    resp = static_file(filepath, root=str(media_dir))
    # Cache for 1 year (immutable content)
    resp.set_header("Cache-Control", "public, max-age=31536000, immutable")
    return resp


@app.route("/telegraph/css/<filepath:path>")
def serve_telegraph_css(filepath):
    """Serve telegraph CSS files with cache headers."""
    css_dir = DATA_DIR / "telegraph" / "css"
    resp = static_file(filepath, root=css_dir)
    # Cache for 1 year (content-addressed, immutable)
    resp.set_header("Cache-Control", "public, max-age=31536000, immutable")
    return resp


@app.route("/telegraph/<channel_id>/<slug>.html")
def serve_telegraph_page(channel_id, slug):
    """Serve downloaded telegraph HTML pages."""
    telegraph_dir = DATA_DIR / "telegraph" / channel_id
    resp = static_file(f"{slug}.html", root=telegraph_dir)
    # Cache for 1 day (content may be re-downloaded if updated)
    resp.set_header("Cache-Control", "public, max-age=86400")
    return resp


@app.route("/static/<filepath:path>")
def serve_static(filepath):
    """Serve static files with cache headers."""
    resp = static_file(filepath, root=STATIC_DIR)
    # Cache for 1 week
    resp.set_header("Cache-Control", "public, max-age=604800")
    return resp


@app.route("/api/channel/<channel_id:int>/photo")
def get_channel_photo(channel_id):
    """Serve channel photo with cache headers."""
    photos_dir = DATA_DIR / "photos"
    photo_path = photos_dir / f"{channel_id}.jpg"
    if photo_path.exists():
        resp = static_file(f"{channel_id}.jpg", root=photos_dir)
        # Cache for 1 day (photos may update)
        resp.set_header("Cache-Control", "public, max-age=86400")
        return resp
    # Return a 1x1 transparent pixel if no photo
    response.status = 404
    return ""


@app.route("/api/download-media/<channel_id:int>/<message_id:int>", method="POST")
def download_media(channel_id, message_id):
    """Download media immediately from Telegram."""
    import concurrent.futures
    import threading

    logger.info(f"Download media request: channel={channel_id}, message={message_id}")
    response.content_type = "application/json"

    with Database() as db:
        msg = db.get_message(channel_id, message_id)
        if not msg:
            response.status = 404
            return json.dumps({"error": "Message not found"})

        if msg.get("media_path"):
            return json.dumps({"path": msg["media_path"]})

        if not msg.get("media_type"):
            response.status = 404
            return json.dumps({"error": "No media in this message"})

        channel = db.get_channel_by_id(channel_id)
        if not channel:
            response.status = 404
            return json.dumps({"error": "Channel not found"})

    access_hash = channel["access_hash"]
    dest_dir = str(MEDIA_DIR)

    # Create pause file to signal sync scripts to pause
    try:
        PAUSE_FILE.touch()
        logger.info(f"Created pause file: {PAUSE_FILE}")
    except Exception as e:
        logger.warning(f"Could not create pause file: {e}")

    def do_download():
        """Run download in a new event loop in this thread."""
        import asyncio
        from tg_client import TGClient, TGClientConnectionError

        logger.info(f"do_download started in thread")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def download_async():
                try:
                    logger.info(f"Connecting to TGClient...")
                    async with TGClient() as client:
                        logger.info(f"Connected, calling download_media...")
                        result = await client.download_media(
                            channel_id, access_hash, message_id, dest_dir
                        )
                        logger.info(f"download_media returned: {result}")
                        return result
                except TGClientConnectionError as e:
                    logger.error(f"TGClientConnectionError: {e}")
                    return {"error": "Daemon not running"}
                except Exception as e:
                    logger.error(f"Download error: {e}")
                    return {"error": str(e)}

            return loop.run_until_complete(download_async())
        finally:
            loop.close()
            logger.info(f"do_download thread finished")

    # Run in thread pool - sync should pause due to pause file
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(do_download)
            result = future.result()  # No timeout - large files may take hours
    except Exception as e:
        logger.error(f"Download failed: {e}")
        response.status = 503
        return json.dumps({"error": str(e)})
    finally:
        # Remove pause file
        try:
            if PAUSE_FILE.exists():
                PAUSE_FILE.unlink()
                logger.info(f"Removed pause file")
        except Exception as e:
            logger.warning(f"Could not remove pause file: {e}")

    if not result:
        response.status = 503
        return json.dumps({"error": "Download returned no result"})

    if "error" in result:
        response.status = 503
        return json.dumps(result)

    if result.get("path"):
        with Database() as db:
            db.update_message_media(channel_id, message_id, result["path"], media_pending=0)
        logger.info(f"Downloaded media: {result['path']}")
        return json.dumps({"path": result["path"]})

    return json.dumps({"error": "Download failed"})


@app.route("/api/group/<group_id:int>/messages")
def get_group_messages(group_id):
    """Get unread messages for a group, optionally filtered to a single channel."""
    response.content_type = "application/json"
    limit = int(request.query.get("limit", 100))
    channel_id = request.query.get("channel")
    channel_id = int(channel_id) if channel_id else None
    with Database() as db:
        messages = db.get_unread_messages_by_group(group_id, limit, channel_id)
        return json.dumps(messages)


@app.route("/api/group/<group_id:int>/earlier")
def get_earlier_messages(group_id):
    """Get earlier (read) messages for a group, before a given date."""
    response.content_type = "application/json"
    before_date = int(request.query.get("before", 0))
    limit = int(request.query.get("limit", 50))
    channel_id = request.query.get("channel")
    channel_id = int(channel_id) if channel_id else None
    if before_date <= 0:
        return json.dumps([])
    with Database() as db:
        messages = db.get_earlier_messages_by_group(group_id, before_date, limit, channel_id)
        return json.dumps(messages)


@app.route("/api/channel/<channel_id:int>/oldest")
def get_oldest_messages(channel_id):
    """Get the oldest messages for a channel."""
    response.content_type = "application/json"
    limit = int(request.query.get("limit", 50))
    with Database() as db:
        messages = db.get_oldest_messages(channel_id, limit)
        return json.dumps(messages)


@app.route("/api/channel/<channel_id:int>/later")
def get_later_messages(channel_id):
    """Get messages newer than a given date for a channel."""
    response.content_type = "application/json"
    after_date = int(request.query.get("after", 0))
    limit = int(request.query.get("limit", 50))
    if after_date <= 0:
        return json.dumps([])
    with Database() as db:
        messages = db.get_later_messages(channel_id, after_date, limit)
        return json.dumps(messages)


@app.route("/api/messages/read", method="POST")
def mark_messages_read():
    """Mark messages as read."""
    response.content_type = "application/json"
    total_start = time.time()

    data = request.json
    messages = data.get("messages", [])  # List of {channel_id, message_id}
    logger.info(f"[/messages/read] Received {len(messages)} messages to mark as read")

    db_start = time.time()
    with Database() as db:
        logger.info(f"[/messages/read] DB connection opened in {time.time() - db_start:.3f}s")

        mark_start = time.time()
        db.mark_messages_read([(m["channel_id"], m["message_id"]) for m in messages])
        logger.info(f"[/messages/read] mark_messages_read() took {time.time() - mark_start:.3f}s")

        commit_start = time.time()
        db.commit()
        logger.info(f"[/messages/read] commit() took {time.time() - commit_start:.3f}s")

    logger.info(f"[/messages/read] Total time: {time.time() - total_start:.3f}s")
    return json.dumps({"success": True})


@app.route("/api/message/rate", method="POST")
def rate_message():
    """Set message rating."""
    response.content_type = "application/json"
    data = request.json
    channel_id = int(data.get("channel_id"))
    message_id = int(data.get("message_id"))
    rating = int(data.get("rating", 0))  # -1, 0, or 1
    with Database() as db:
        db.update_message_rating(channel_id, message_id, rating)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/message/bookmark", method="POST")
def bookmark_message():
    """Set message bookmark status."""
    response.content_type = "application/json"
    data = request.json
    channel_id = int(data.get("channel_id"))
    message_id = int(data.get("message_id"))
    bookmarked = int(data.get("bookmarked", 0))  # 0 or 1
    with Database() as db:
        db.update_message_bookmark(channel_id, message_id, bookmarked)
        db.commit()
    return json.dumps({"success": True})


@app.route("/api/message/<channel_id:int>/<message_id:int>")
def get_message(channel_id, message_id):
    """Get a single message by channel and message ID."""
    response.content_type = "application/json"
    with Database() as db:
        msg = db.get_message(channel_id, message_id)
        if msg:
            return json.dumps(msg)
        response.status = 404
        return json.dumps({"error": "Message not found"})


@app.route("/api/bookmarks")
def get_bookmarks():
    """Get all bookmarked messages."""
    response.content_type = "application/json"
    limit = int(request.query.get("limit", 100))
    with Database() as db:
        messages = db.get_all_bookmarked_messages(limit)
        return json.dumps(messages)


@app.route("/api/search")
def search_messages():
    """Search messages using full-text search.

    Query parameters:
        q: Search query (required, min 3 characters)
        limit: Max results (default 50, max 200)
        channel: Optional channel ID filter
        group: Optional group ID filter
    """
    from urllib.parse import parse_qs, urlparse

    response.content_type = "application/json; charset=utf-8"

    # Parse query string manually to ensure proper UTF-8 decoding
    query_string = request.query_string
    if isinstance(query_string, bytes):
        query_string = query_string.decode('utf-8')
    params = parse_qs(query_string, encoding='utf-8')

    query = params.get("q", [""])[0].strip()
    logger.info(f"[search] Query string: {query_string}, Parsed query: {repr(query)}")

    if not query:
        return json.dumps({"error": "Query parameter 'q' is required", "results": []})
    if len(query) < 3:
        return json.dumps({"error": "Query must be at least 3 characters", "results": []})

    limit = min(int(params.get("limit", [50])[0]), 200)
    channel_id = params.get("channel", [None])[0]
    channel_id = int(channel_id) if channel_id else None
    group_id = params.get("group", [None])[0]
    group_id = int(group_id) if group_id else None

    with Database() as db:
        # Log index stats for debugging
        stats = db.get_search_index_stats()
        logger.info(f"[search] Query: '{query}', Index stats: {stats}")

        results = db.search_messages(query, limit, channel_id, group_id)
        logger.info(f"[search] Found {len(results)} results")

        # Fetch full message data for each result
        enriched_results = []
        for result in results:
            msg = db.get_message(result["channel_id"], result["message_id"])
            if msg:
                msg["channel_title"] = result["channel_title"]
                msg["channel_username"] = result.get("channel_username")
                msg["channel_id"] = result["channel_id"]
                msg["search_query"] = query  # Pass query for frontend highlighting
                # Handle media items for consistency with other endpoints
                if msg.get("media_path"):
                    msg["media_items"] = [{
                        "path": msg["media_path"],
                        "type": msg.get("media_type"),
                        "message_id": msg["id"],
                        "video_thumbnail_path": msg.get("video_thumbnail_path")
                    }]
                else:
                    msg["media_items"] = []
                msg["is_album"] = False
                msg["album_message_ids"] = [msg["id"]]
                enriched_results.append(msg)

        return json.dumps({"results": enriched_results, "query": query})


@app.route("/api/search/stats")
def search_stats():
    """Get search index statistics."""
    response.content_type = "application/json"
    with Database() as db:
        stats = db.get_search_index_stats()
        return json.dumps(stats)


def main():
    """Run the web server."""
    print(f"Starting TGFeed Web UI at http://{WEB_HOST}:{WEB_PORT}")
    app.run(host=WEB_HOST, port=WEB_PORT, server="waitress")


if __name__ == "__main__":
    main()
