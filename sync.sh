#!/bin/bash
cd "$(dirname "$0")"
while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting sync..."
    uv run python sync_read_to_tg.py
    uv run python sync_channels.py
    uv run python sync_messages.py
    # uv run python cleanup.py
    # uv run python generate_thumbnails.py
    # uv run python generate_content_hashes.py
    uv run python download_telegraph.py
done
