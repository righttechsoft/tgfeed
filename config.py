"""Configuration for TGFeed."""

import os
from pathlib import Path

from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
load_dotenv(SCRIPT_DIR / ".env")

# Telegram API credentials
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")

# Paths
DATA_DIR = SCRIPT_DIR / "data"
SESSION_PATH = DATA_DIR / "session"  # Legacy single-session path
SESSIONS_DIR = DATA_DIR / "sessions"  # Per-credential session files
MEDIA_DIR = DATA_DIR / "media"
DATABASE_PATH = DATA_DIR / "tgfeed.db"

# Telegram daemon settings
TG_DAEMON_HOST = os.getenv("TG_DAEMON_HOST", "127.0.0.1")
TG_DAEMON_PORT = int(os.getenv("TG_DAEMON_PORT", "9876"))

# Mistral API (for content deduplication)
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-small-latest")

# Deduplication settings
DEDUP_MIN_MESSAGE_LENGTH = int(os.getenv("DEDUP_MIN_MESSAGE_LENGTH", "50"))
DEDUP_MESSAGES_PER_RUN = int(os.getenv("DEDUP_MESSAGES_PER_RUN", "100"))


def validate_config() -> None:
    """Validate required configuration."""
    if not API_ID:
        raise ValueError("API_ID not set in .env")
    if not API_HASH:
        raise ValueError("API_HASH not set in .env")
    if not PHONE_NUMBER:
        raise ValueError("PHONE_NUMBER not set in .env")
