#!/bin/bash
cd "$(dirname "$0")"
while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting sync..."
    uv run python cleanup.py
    uv run python generate_thumbnails.py
    uv run python generate_content_hashes.py
done
