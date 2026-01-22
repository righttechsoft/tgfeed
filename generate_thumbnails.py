"""Generate video thumbnails for TGFeed.

Creates 2x2 grid thumbnails from video files using ffmpeg.
Processes videos without thumbnails, starting from the newest.
"""

import logging
import subprocess
import sys
from pathlib import Path

from config import MEDIA_DIR, validate_config
from database import Database, DatabaseMigration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Configuration
THUMBNAILS_PER_RUN = 50  # Process this many videos per run
FRAME_WIDTH = 320  # Width of each frame in the grid


def get_video_duration(video_path: Path) -> float | None:
    """Get video duration in seconds using ffprobe."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path)
            ],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception as e:
        logger.debug(f"Failed to get duration for {video_path}: {e}")
    return None


def generate_thumbnail(video_path: Path, output_path: Path) -> bool:
    """Generate a 2x2 grid thumbnail from a video using ffmpeg.

    Extracts 4 frames at 10%, 30%, 50%, 70% of the video duration
    and combines them into a 2x2 grid.
    """
    duration = get_video_duration(video_path)
    if not duration or duration < 1:
        logger.warning(f"Video too short or invalid duration: {video_path}")
        return False

    # Calculate timestamps for 4 frames (10%, 30%, 50%, 70%)
    timestamps = [duration * p for p in [0.1, 0.3, 0.5, 0.7]]

    try:
        # Extract 4 frames to temporary files
        temp_frames = []
        for i, ts in enumerate(timestamps):
            frame_path = output_path.parent / f"_temp_frame_{output_path.stem}_{i}.jpg"
            result = subprocess.run(
                [
                    "ffmpeg", "-y",
                    "-ss", str(ts),
                    "-i", str(video_path),
                    "-frames:v", "1",
                    "-update", "1",
                    "-q:v", "2",
                    "-vf", f"scale={FRAME_WIDTH}:-1",
                    str(frame_path)
                ],
                capture_output=True,
                timeout=30
            )
            if frame_path.exists():
                temp_frames.append(frame_path)
            else:
                logger.debug(f"Failed to extract frame {i} at {ts}s")

        if len(temp_frames) < 4:
            # Cleanup and fail
            for f in temp_frames:
                f.unlink(missing_ok=True)
            logger.warning(f"Could only extract {len(temp_frames)}/4 frames from {video_path}")
            return False

        # Combine into 2x2 grid using ffmpeg xstack filter
        # xstack is more flexible than hstack/vstack - handles different sizes
        # Scale all frames to fixed 320x180 with padding to ensure uniform size
        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", str(temp_frames[0]),
                "-i", str(temp_frames[1]),
                "-i", str(temp_frames[2]),
                "-i", str(temp_frames[3]),
                "-filter_complex",
                "[0]scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2,setsar=1[s0];"
                "[1]scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2,setsar=1[s1];"
                "[2]scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2,setsar=1[s2];"
                "[3]scale=320:180:force_original_aspect_ratio=decrease,pad=320:180:(ow-iw)/2:(oh-ih)/2,setsar=1[s3];"
                "[s0][s1][s2][s3]xstack=inputs=4:layout=0_0|w0_0|0_h0|w0_h0",
                "-frames:v", "1",
                "-update", "1",
                "-q:v", "2",
                str(output_path)
            ],
            capture_output=True,
            timeout=30
        )

        # Cleanup temp frames
        for f in temp_frames:
            f.unlink(missing_ok=True)

        if output_path.exists():
            return True
        else:
            logger.warning(f"Failed to create grid thumbnail for {video_path}")
            if result.stderr:
                # Get last 1000 chars of stderr (the actual error is usually at the end)
                stderr_text = result.stderr.decode('utf-8', errors='replace')
                logger.warning(f"  ffmpeg error (last 1000 chars): ...{stderr_text[-1000:]}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout generating thumbnail for {video_path}")
        return False
    except Exception as e:
        logger.error(f"Error generating thumbnail for {video_path}: {e}")
        return False


def generate_thumbnails() -> None:
    """Main function to generate video thumbnails."""
    logger.info("Starting video thumbnail generation...")

    validate_config()

    # Run migrations to ensure video_thumbnail_path column exists
    DatabaseMigration().migrate()

    # Get active channels
    with Database() as db:
        channels = [dict(row) for row in db.get_active_channels()]

    if not channels:
        logger.info("No active channels found")
        return

    logger.info(f"Found {len(channels)} active channels")

    total_generated = 0
    total_failed = 0

    for channel in channels:
        channel_id = channel["id"]
        channel_title = channel["title"]

        # Get videos without thumbnails (newest first)
        with Database() as db:
            videos = db.get_videos_without_thumbnails(channel_id, limit=THUMBNAILS_PER_RUN)

        if not videos:
            continue

        logger.info(f"Processing {len(videos)} videos from: {channel_title}")

        for video in videos:
            video_path = MEDIA_DIR / video["media_path"]

            if not video_path.exists():
                logger.warning(f"  Video file not found: {video_path}")
                continue

            # Thumbnail goes in same directory as video
            thumb_filename = f"{video_path.stem}_thumb.jpg"
            thumb_path = video_path.parent / thumb_filename

            # Relative path for database (same structure as media_path)
            thumb_relative = str(Path(video["media_path"]).parent / thumb_filename)

            if generate_thumbnail(video_path, thumb_path):
                with Database() as db:
                    db.update_video_thumbnail(channel_id, video["id"], thumb_relative)
                    db.commit()
                total_generated += 1
                logger.info(f"  Generated: {thumb_relative}")
            else:
                total_failed += 1

    logger.info("=" * 50)
    logger.info(f"Thumbnail generation completed!")
    logger.info(f"  Generated: {total_generated}")
    logger.info(f"  Failed: {total_failed}")
    logger.info("=" * 50)


if __name__ == "__main__":
    generate_thumbnails()
