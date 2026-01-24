"""Download telegra.ph pages referenced in messages."""

import base64
import hashlib
import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from config import DATA_DIR, validate_config
from database import Database, DatabaseMigration

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

# Directory for downloaded telegraph pages
TELEGRAPH_DIR = DATA_DIR / "telegraph"
CSS_DIR = TELEGRAPH_DIR / "css"

# Regex to find telegra.ph URLs
TELEGRAPH_URL_PATTERN = re.compile(r'https?://telegra\.ph/[^\s"<>]+')


def extract_telegraph_urls(entities_json: str | None, message_text: str | None) -> list[str]:
    """Extract telegra.ph URLs from message entities and text."""
    urls = set()

    # Check entities JSON for URLs
    if entities_json:
        try:
            entities = json.loads(entities_json)
            for entity in entities:
                url = entity.get("url", "")
                if "telegra.ph" in url:
                    urls.add(url)
        except (json.JSONDecodeError, TypeError):
            pass

    # Also scan message text for URLs
    if message_text:
        matches = TELEGRAPH_URL_PATTERN.findall(message_text)
        urls.update(matches)

    return list(urls)


def download_and_embed_image(img_url: str, session: requests.Session) -> str | None:
    """Download image and return as base64 data URI."""
    try:
        resp = session.get(img_url, timeout=30)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "image/jpeg")
        b64_data = base64.b64encode(resp.content).decode("utf-8")
        return f"data:{content_type};base64,{b64_data}"
    except Exception as e:
        logger.warning(f"    Failed to download image {img_url}: {e}")
        return None


def get_or_download_css(css_url: str, session: requests.Session) -> str | None:
    """Download CSS, save with content hash, return local path.

    If CSS with same content already exists, reuse it.
    Returns the filename (without path) for use in HTML.
    """
    try:
        resp = session.get(css_url, timeout=30)
        resp.raise_for_status()
        css_content = resp.text

        # Embed images referenced in CSS as base64
        css_url_pattern = re.compile(r'url\(["\']?([^)"\']+)["\']?\)')

        def replace_css_url(m):
            resource_url = urljoin(css_url, m.group(1))
            if resource_url.startswith("data:"):
                return m.group(0)
            embedded = download_and_embed_image(resource_url, session)
            if embedded:
                return f'url("{embedded}")'
            return m.group(0)

        css_content = css_url_pattern.sub(replace_css_url, css_content)

        # Generate hash of the content
        content_hash = hashlib.md5(css_content.encode("utf-8")).hexdigest()[:12]
        css_filename = f"{content_hash}.css"
        css_path = CSS_DIR / css_filename

        # Only write if doesn't exist (deduplication)
        if not css_path.exists():
            CSS_DIR.mkdir(parents=True, exist_ok=True)
            css_path.write_text(css_content, encoding="utf-8")
            logger.info(f"    Saved new CSS: {css_filename}")
        else:
            logger.info(f"    Reusing existing CSS: {css_filename}")

        return css_filename

    except Exception as e:
        logger.warning(f"    Failed to download CSS {css_url}: {e}")
        return None


def download_telegraph_page(url: str, output_dir: Path) -> bool:
    """Download a telegra.ph page with images embedded and CSS linked."""
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

        # Download the HTML
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Extract page slug for filename
        parsed = urlparse(url)
        slug = parsed.path.strip("/").replace("/", "_")
        if not slug:
            slug = "index"

        # Remove the websync script
        html = re.sub(
            r'<script[^>]*src=["\'][^"\']*t\.me/_websync_[^"\']*["\'][^>]*>\s*</script>',
            '',
            html,
            flags=re.IGNORECASE
        )

        # Also remove inline script that references websync
        html = re.sub(
            r'<script[^>]*>[^<]*t\.me/_websync_[^<]*</script>',
            '',
            html,
            flags=re.IGNORECASE | re.DOTALL
        )

        # Find and process CSS - replace with local paths
        # Match both href before rel and rel before href
        css_pattern = re.compile(r'<link[^>]+href=["\']([^"\']+\.css[^"\']*)["\'][^>]*>', re.IGNORECASE)
        for match in css_pattern.finditer(html):
            css_url = urljoin(url, match.group(1))
            css_filename = get_or_download_css(css_url, session)

            if css_filename:
                # Replace with local CSS path (served by web server)
                new_link = f'<link rel="stylesheet" href="/telegraph/css/{css_filename}">'
                html = html.replace(match.group(0), new_link)

        # Find and embed images
        img_pattern = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
        for match in img_pattern.finditer(html):
            img_src = match.group(1)
            if img_src.startswith("data:"):
                continue

            img_url = urljoin(url, img_src)
            embedded = download_and_embed_image(img_url, session)
            if embedded:
                html = html.replace(f'src="{img_src}"', f'src="{embedded}"')
                html = html.replace(f"src='{img_src}'", f'src="{embedded}"')

        # Also handle background images in style attributes
        style_bg_pattern = re.compile(r'style=["\'][^"\']*background[^:]*:\s*url\(["\']?([^)"\']+)["\']?\)', re.IGNORECASE)
        for match in style_bg_pattern.finditer(html):
            bg_url = match.group(1)
            if bg_url.startswith("data:"):
                continue

            full_url = urljoin(url, bg_url)
            embedded = download_and_embed_image(full_url, session)
            if embedded:
                html = html.replace(bg_url, embedded)

        # Handle figure images (telegra.ph uses these)
        figure_pattern = re.compile(r'<figure[^>]*>.*?<img[^>]+src=["\']([^"\']+)["\'].*?</figure>', re.IGNORECASE | re.DOTALL)
        for match in figure_pattern.finditer(html):
            img_src = match.group(1)
            if img_src.startswith("data:"):
                continue

            img_url = urljoin(url, img_src)
            embedded = download_and_embed_image(img_url, session)
            if embedded:
                html = html.replace(f'src="{img_src}"', f'src="{embedded}"')
                html = html.replace(f"src='{img_src}'", f'src="{embedded}"')

        # Save the HTML file
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{slug}.html"
        output_file.write_text(html, encoding="utf-8")

        logger.info(f"    Saved: {output_file.name}")
        return True

    except Exception as e:
        logger.error(f"    Failed to download {url}: {e}")
        return False


def download_telegraph_pages() -> None:
    """Download all telegra.ph pages from messages."""
    logger.info("Starting telegra.ph download...")

    validate_config()

    # Run migrations to add html_downloaded column
    DatabaseMigration().migrate()

    # Get all channel tables
    with Database() as db:
        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'channel_%'")
        tables = [row[0] for row in cursor.fetchall() if not row[0].startswith("channel_backup_hash_")]

    if not tables:
        logger.info("No channel tables found")
        return

    total_downloaded = 0
    total_failed = 0

    for table_name in tables:
        channel_id = table_name.replace("channel_", "")

        try:
            with Database() as db:
                cursor = db.conn.cursor()

                # Find messages with telegra.ph URLs that haven't been downloaded
                # Check both entities JSON and message text
                cursor.execute(f"""
                    SELECT id, message, entities
                    FROM {table_name}
                    WHERE (html_downloaded = 0 OR html_downloaded IS NULL)
                    AND (
                        entities LIKE '%telegra.ph%'
                        OR message LIKE '%telegra.ph%'
                    )
                    LIMIT 10
                """)
                messages = cursor.fetchall()

                if not messages:
                    continue

                logger.info(f"Channel {channel_id}: {len(messages)} messages with telegra.ph links")

                for msg in messages:
                    msg_id = msg["id"]
                    message_text = msg["message"]
                    entities_json = msg["entities"]

                    # Extract telegra.ph URLs
                    urls = extract_telegraph_urls(entities_json, message_text)

                    if not urls:
                        # No valid URLs found, mark as processed
                        cursor.execute(
                            f"UPDATE {table_name} SET html_downloaded = 1 WHERE id = ?",
                            (msg_id,)
                        )
                        db.commit()
                        continue

                    # Download each URL
                    output_dir = TELEGRAPH_DIR / channel_id
                    all_success = True

                    for url in urls:
                        logger.info(f"  Downloading: {url}")
                        if download_telegraph_page(url, output_dir):
                            total_downloaded += 1
                        else:
                            all_success = False
                            total_failed += 1

                    # Only mark as downloaded if all URLs succeeded
                    if all_success:
                        cursor.execute(
                            f"UPDATE {table_name} SET html_downloaded = 1 WHERE id = ?",
                            (msg_id,)
                        )
                        db.commit()

        except Exception as e:
            logger.error(f"Error processing {table_name}: {e}")
            continue

    logger.info("=" * 50)
    logger.info(f"Telegraph download completed: {total_downloaded} pages downloaded, {total_failed} failed")
    logger.info("=" * 50)


if __name__ == "__main__":
    download_telegraph_pages()
