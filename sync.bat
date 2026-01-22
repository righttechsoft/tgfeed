@echo off
cd /d "%~dp0"
:loop
echo [%date% %time%] Starting sync...
uv run python sync_read_to_tg.py 
uv run python sync_channels.py
uv run python sync_messages.py
:uv run python cleanup.py
:uv run python generate_thumbnails.py
:uv run python generate_content_hashes.py
uv run python download_telegraph.py
goto loop
