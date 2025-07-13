CHANNEL_ID= ""
STREAM_KEY = ""  # Replace with your key

import os
import time
import random
import logging
from datetime import datetime
import googleapiclient.discovery
import yt_dlp
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import subprocess
import threading
import re

# Configuration
CLIENT_SECRETS_FILE = "client_secrets.json"
SCOPES = ["https://www.googleapis.com/auth/youtube"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
TOKEN_FILE = "token.json"


# Stream settings
DOWNLOAD_DIR = "./downloads"
MORNING_HOURS = (2, 11)  # 5AM to 7AM
MIN_DURATION = 6 * 3600  # 6 hours minimum
FRAMERATE = 30
INTRA = 60  # Keyframe interval
PREPARE_NEXT_AFTER = 600  # 10 minutes in seconds
MAX_RETRIES = 3
CLEANUP_OLDER_THAN = 24 * 3600  # Cleanup files older than 24 hours

class StreamManager:
    def __init__(self):
        """Initialize the StreamManager with verbose logging"""
        self.current_stream = None
        self.next_stream = None
        self.ffmpeg_process = None
        self.youtube = None
        self.stop_event = threading.Event()
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        logging.info("StreamManager initialized")
        logging.info(f"Download directory: {os.path.abspath(DOWNLOAD_DIR)}")
        logging.info(f"Looking for streams between {MORNING_HOURS[0]}:00 and {MORNING_HOURS[1]}:00")
        logging.info(f"Minimum stream duration: {MIN_DURATION//3600} hours")

    def authenticate(self):
        """Authenticate with YouTube API with detailed logging"""
        logging.info("Starting authentication process")

        creds = None
        if os.path.exists(TOKEN_FILE):
            logging.info("Found existing token file")
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            logging.info("Loaded credentials from token file")

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Credentials expired, refreshing...")
                creds.refresh(Request())
                logging.info("Successfully refreshed credentials")
            else:
                logging.info("No valid credentials found, starting OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRETS_FILE, SCOPES)
                logging.info("Please complete the OAuth flow in your browser")
                creds = flow.run_local_server(port=0)
                logging.info("Successfully obtained credentials")

            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            logging.info("Saved credentials to token file")

        logging.info("Building YouTube API service")
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds)
        logging.info("Successfully authenticated with YouTube API")
        return service

    def parse_duration(self, duration):
        """Parse ISO 8601 duration to seconds with verbose output"""
        logging.debug(f"Parsing duration string: {duration}")
        match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
        if not match:
            logging.warning(f"Failed to parse duration: {duration}")
            return 0

        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        total_seconds = hours * 3600 + minutes * 60 + seconds
        logging.debug(f"Parsed duration: {hours}h {minutes}m {seconds}s = {total_seconds} seconds")
        return total_seconds

    def find_random_long_stream(self):
        """Find one random live stream that meets criteria"""
        logging.info("Starting search for eligible live streams")

        try:
            logging.info("Searching for live broadcasts from channel")
            search_response = self.youtube.search().list(
                channelId=CHANNEL_ID,
                part="id,snippet",
                type="video",
                eventType="completed",  # Search completed live streams
                maxResults=50,
                order="date"  # Most recent first
            ).execute()

            logging.info(f"Found {len(search_response['items'])} live streams")

            # Filter for morning streams
            candidates = []
            for item in search_response["items"]:
                try:
                    published_at = item["snippet"]["publishedAt"]
                    dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
                    logging.debug(f"Checking live stream {item['snippet']['title']} from {dt}")

                    if MORNING_HOURS[0] <= dt.hour < MORNING_HOURS[1]:
                        video_info = {
                            "id": item["id"]["videoId"],
                            "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                            "title": item["snippet"]["title"],
                            "publishedAt": published_at
                        }
                        candidates.append(video_info)
                except Exception as e:
                    logging.warning(f"Error processing live stream: {e}")
                    continue

            if not candidates:
                logging.warning("No morning live streams found")
                return None

            logging.info(f"Found {len(candidates)} potential morning live streams")
            stream = random.choice(candidates)
            logging.info(f"Selected random live stream: {stream['title']}")

            # Verify duration (1 additional API call)
            video_response = self.youtube.videos().list(
                id=stream["id"],
                part="contentDetails"
            ).execute()

            duration = self.parse_duration(video_response["items"][0]["contentDetails"]["duration"])
            logging.info(f"Live stream duration: {duration//3600}h {duration%3600//60}m")

            if duration >= MIN_DURATION:
                logging.info("Live stream meets all criteria")
                return stream

            logging.warning("Live stream duration too short")
            return None

        except Exception as e:
            logging.error(f"Error finding live stream: {e}", exc_info=True)
            return None

    def download_stream(self, stream):
        """Download stream with retries and detailed progress"""
        filename = os.path.join(DOWNLOAD_DIR, f"{stream['id']}.mp4")
        logging.info(f"Starting download for: {stream['title']}")
        logging.info(f"Destination file: {filename}")

        if os.path.exists(filename):
            logging.info("File already exists, skipping download")
            return filename

        ydl_opts = {
            'outtmpl': filename,
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'quiet': False,  # Make yt-dlp verbose
            'retries': 3,
            'progress_hooks': [self.download_progress_hook],
        }

        for attempt in range(MAX_RETRIES):
            try:
                logging.info(f"Download attempt {attempt + 1} of {MAX_RETRIES}")
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([stream['url']])
                logging.info("Download completed successfully")
                return filename
            except Exception as e:
                logging.warning(f"Download attempt {attempt + 1} failed: {e}")
                if os.path.exists(filename):
                    logging.info("Removing partially downloaded file")
                    os.remove(filename)
                if attempt == MAX_RETRIES - 1:
                    logging.error("Max download attempts reached")
                    return None
                logging.info("Waiting 5 seconds before retry...")
                time.sleep(5)

    def download_progress_hook(self, d):
        """Callback for download progress"""
        if d['status'] == 'downloading':
            percent = d.get('_percent_str', 'N/A')
            speed = d.get('_speed_str', 'N/A')
            eta = d.get('_eta_str', 'N/A')
            logging.debug(f"Download progress: {percent} at {speed}, ETA: {eta}")
        elif d['status'] == 'finished':
            logging.debug("Download finished, post-processing now")

    def start_stream(self, input_file):
        """Start FFmpeg streaming with detailed logging"""
        logging.info(f"Preparing to stream file: {input_file}")

        if not os.path.exists(input_file):
            logging.error("Input file does not exist")
            return False

        file_size = os.path.getsize(input_file) / (1024 * 1024)  # in MB
        logging.info(f"File size: {file_size:.2f} MB")

        cmd = [
            'ffmpeg',
            '-v', 'info',  # Make FFmpeg verbose
            '-re',  # Read input at native frame rate
            '-i', input_file,
            '-f', 'lavfi',
            '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-g', str(INTRA),
            '-f', 'flv',
            f"rtmp://a.rtmp.youtube.com/live2/{STREAM_KEY}"
        ]

        logging.info("Starting FFmpeg with command:")
        logging.info(' '.join(cmd))

        self.ffmpeg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )

        # Start thread to log FFmpeg output
        threading.Thread(
            target=self.log_ffmpeg_output,
            daemon=True
        ).start()

        logging.info("FFmpeg process started successfully")
        return True

    def log_ffmpeg_output(self):
        """Log FFmpeg output in real-time"""
        logging.info("Starting FFmpeg output logger")
        for line in iter(self.ffmpeg_process.stdout.readline, ''):
            logging.debug(f"FFmpeg: {line.strip()}")
        logging.info("FFmpeg process ended")

    def get_duration(self, filename):
        """Get video duration with detailed logging"""
        logging.info(f"Getting duration for {filename}")
        try:
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries',
                 'format=duration', '-of', 'csv=p=0', filename],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            duration = float(result.stdout.strip())
            logging.info(f"Video duration: {duration} seconds ({duration/60:.1f} minutes)")
            return duration
        except Exception as e:
            logging.error(f"Error getting duration: {e}")
            return MIN_DURATION  # Default if can't determine

    def cleanup_old_files(self):
        """Cleanup old files with logging"""
        logging.info("Starting cleanup of old files")
        now = time.time()
        removed_count = 0

        for filename in os.listdir(DOWNLOAD_DIR):
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            try:
                if os.path.isfile(filepath):
                    file_age = now - os.path.getmtime(filepath)
                    if file_age > CLEANUP_OLDER_THAN:
                        logging.info(f"Removing old file: {filename} (age: {file_age/3600:.1f} hours)")
                        os.remove(filepath)
                        removed_count += 1
            except Exception as e:
                logging.warning(f"Error cleaning up file {filename}: {e}")
                continue

        logging.info(f"Cleanup complete. Removed {removed_count} files")

    def run(self):
        """Main streaming loop with detailed operation logging"""
        logging.info("Starting main streaming loop")

        self.youtube = self.authenticate()
        if not self.youtube:
            logging.error("Failed to authenticate with YouTube API")
            return

        while not self.stop_event.is_set():
            try:
                logging.info("\n" + "="*50)
                logging.info("Starting new streaming cycle")
                logging.info("="*50 + "\n")

                # Find and download initial stream
                logging.info("Searching for a suitable stream...")
                self.current_stream = self.find_random_long_stream()
                if not self.current_stream:
                    logging.warning("No suitable stream found, waiting 60 seconds")
                    time.sleep(60)
                    continue

                logging.info(f"Found stream: {self.current_stream['title']}")
                logging.info(f"Stream URL: {self.current_stream['url']}")
                logging.info("Starting download...")

                current_file = self.download_stream(self.current_stream)
                if not current_file:
                    logging.error("Download failed, waiting 60 seconds")
                    time.sleep(60)
                    continue

                # Start streaming
                logging.info("Starting FFmpeg streaming process")
                if not self.start_stream(current_file):
                    logging.error("Failed to start streaming, waiting 60 seconds")
                    time.sleep(60)
                    continue

                duration = self.get_duration(current_file)
                logging.info(f"Streaming {self.current_stream['title']} for {duration//3600}h {duration%3600//60}m")

                # Schedule next stream preparation
                def prepare_next():
                    logging.info(f"Waiting {PREPARE_NEXT_AFTER//60} minutes to prepare next stream")
                    time.sleep(PREPARE_NEXT_AFTER)

                    logging.info("Preparing next stream...")
                    self.next_stream = self.find_random_long_stream()
                    if self.next_stream:
                        logging.info(f"Found next stream: {self.next_stream['title']}")
                        self.download_stream(self.next_stream)
                    else:
                        logging.warning("No next stream found")

                    self.cleanup_old_files()

                threading.Thread(target=prepare_next, daemon=True).start()

                # Wait for current stream to finish
                start_time = time.time()
                estimated_end = start_time + duration
                logging.info(f"Stream started at {time.ctime(start_time)}")
                logging.info(f"Estimated end time: {time.ctime(estimated_end)}")

                while (time.time() - start_time < duration) and not self.stop_event.is_set():
                    elapsed = time.time() - start_time
                    remaining = max(0, duration - elapsed)
                    logging.debug(f"Stream progress: {elapsed//3600:.0f}h {elapsed%3600//60:.0f}m elapsed, "
                                f"{remaining//3600:.0f}h {remaining%3600//60:.0f}m remaining")

                    if self.ffmpeg_process.poll() is not None:
                        logging.warning("FFmpeg process ended prematurely")
                        break
                    time.sleep(30)  # Check every 30 seconds

                # Clean up
                logging.info("End of stream reached, cleaning up...")
                if self.ffmpeg_process:
                    logging.info("Terminating FFmpeg process")
                    self.ffmpeg_process.terminate()
                    self.ffmpeg_process.wait()
                    logging.info("FFmpeg process terminated")

                if self.stop_event.is_set():
                    logging.info("Stop signal received, exiting main loop")
                    break

                if self.next_stream:
                    logging.info("Switching to next prepared stream")
                    self.current_stream = self.next_stream
                    self.next_stream = None
                else:
                    logging.warning("No next stream prepared, waiting 60 seconds")
                    time.sleep(60)

            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
                logging.info("Waiting 60 seconds before continuing")
                time.sleep(60)

    def stop(self):
        """Graceful shutdown with logging"""
        logging.info("Initiating graceful shutdown...")
        self.stop_event.set()
        if self.ffmpeg_process:
            logging.info("Stopping FFmpeg process...")
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=10)
                logging.info("FFmpeg process stopped")
            except subprocess.TimeoutExpired:
                logging.warning("FFmpeg didn't stop gracefully, killing it")
                self.ffmpeg_process.kill()
        logging.info("StreamManager shutdown complete")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('stream_manager.log')
        ]
    )
    logging.info("=== Starting YouTube Stream Manager ===")

    manager = StreamManager()
    try:
        manager.run()
    except KeyboardInterrupt:
        logging.info("\nReceived keyboard interrupt, shutting down...")
        manager.stop()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        logging.info("=== YouTube Stream Manager Stopped ===")
