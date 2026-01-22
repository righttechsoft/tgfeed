#!/bin/bash
cd "$(dirname "$0")"
uv run python tg_daemon.py
