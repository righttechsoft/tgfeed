@echo off
cd /d "%~dp0"
:loop
echo [%date% %time%] Starting sync...
uv run python cleanup.py
uv run python generate_thumbnails.py
uv run python generate_content_hashes.py
goto loop
