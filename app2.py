CHANNEL_ID= ""
STREAM_KEY = ""  # Replace with your key    
SOURCE_CHANNEL_ID = ""

import os
import time
import logging
import subprocess
import threading
from datetime import datetime
import googleapiclient.discovery
import yt_dlp
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# Configuration
CLIENT_SECRETS_FILE = "client_secrets.json"
SCOPES = ["https://www.googleapis.com/auth/youtube"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
TOKEN_FILE = "token.json"

# Stream settings
DOWNLOAD_DIR = "./downloads"
FRAMERATE = 30
INTRA = 60  # Keyframe interval
CHECK_INTERVAL = 30  # Check for live streams every 30 seconds
COMEBACK_IMAGE = "combacklater.png"  # Make sure this file exists

class LiveStreamManager:
    def __init__(self):
        """Initialize the LiveStreamManager"""
        self.current_stream = None
        self.ffmpeg_process = None
        self.youtube = None
        self.stop_event = threading.Event()
        self.last_stream_check = 0
        self.stream_start_time = None
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        logging.info("LiveStreamManager initialized")
        
        # Verify the comeback image exists
        if not os.path.exists(COMEBACK_IMAGE):
            logging.warning(f"Comeback image '{COMEBACK_IMAGE}' not found. Creating a placeholder...")
            self.create_placeholder_image()

    def create_placeholder_image(self):
        """Create a placeholder image if it doesn't exist"""
        try:
            # Create a simple black image with text using FFmpeg
            cmd = [
                'ffmpeg',
                '-f', 'lavfi',
                '-i', 'color=size=1280x720:rate=1:color=black',
                '-vf', "drawtext=text='Come Back Later':fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2",
                '-frames:v', '1',
                '-y',  # Overwrite output file
                COMEBACK_IMAGE
            ]
            subprocess.run(cmd, check=True, capture_output=True)
            logging.info(f"Created placeholder image: {COMEBACK_IMAGE}")
        except Exception as e:
            logging.error(f"Failed to create placeholder image: {e}")

    def authenticate(self):
        """Authenticate with YouTube API"""
        logging.info("Starting authentication process")
        
        creds = None
        if os.path.exists(TOKEN_FILE):
            logging.info("Found existing token file")
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Credentials expired, refreshing...")
                creds.refresh(Request())
            else:
                logging.info("No valid credentials found, starting OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRETS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
            logging.info("Saved credentials to token file")

        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds)
        logging.info("Successfully authenticated with YouTube API")
        return service

    def check_currently_live_streams(self):
        """Check if the source channel has any currently live streams using multiple methods"""
        # Cache results for 10 seconds to avoid excessive API calls
        current_time = time.time()
        if current_time - self.last_stream_check < 10:
            return self.last_stream_result
        
        self.last_stream_check = current_time
        logging.info(f"Checking for currently live streams on channel: {SOURCE_CHANNEL_ID}")
        
        # Method 1: Direct channel live URL
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': False,  # Changed to False to get full info
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Try the direct live URL first
                live_url = f"https://www.youtube.com/channel/{SOURCE_CHANNEL_ID}/live"
                info = ydl.extract_info(live_url, download=False)
                
                if info.get('live_status') == 'is_live':
                    logging.info(f"Found live stream via direct URL: {info.get('title', 'Unknown')}")
                    self.last_stream_result = {
                        'url': info.get('webpage_url', live_url),
                        'title': info.get('title', 'Live Stream'),
                        'id': info.get('id'),
                        'is_live': True,
                        'viewer_count': info.get('concurrent_view_count', 0)
                    }
                    return self.last_stream_result
        except Exception as e:
            logging.debug(f"Direct live URL method failed: {e}")
        
        # Method 2: Search through channel videos for live streams
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,
                'playlistend': 20,  # Check last 20 videos
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                videos_url = f"https://www.youtube.com/channel/{SOURCE_CHANNEL_ID}/videos"
                info = ydl.extract_info(videos_url, download=False)
                
                if 'entries' in info:
                    for entry in info['entries']:
                        if entry.get('live_status') == 'is_live':
                            # Get full info for the live stream
                            try:
                                with yt_dlp.YoutubeDL({'quiet': True}) as ydl_full:
                                    full_info = ydl_full.extract_info(entry.get('url'), download=False)
                                    if full_info.get('live_status') == 'is_live':
                                        logging.info(f"Found live stream in videos: {full_info.get('title', 'Unknown')}")
                                        self.last_stream_result = {
                                            'url': full_info.get('webpage_url', entry.get('url')),
                                            'title': full_info.get('title', 'Live Stream'),
                                            'id': full_info.get('id', entry.get('id')),
                                            'is_live': True,
                                            'viewer_count': full_info.get('concurrent_view_count', 0)
                                        }
                                        return self.last_stream_result
                            except Exception as e:
                                logging.debug(f"Error getting full info for {entry.get('url')}: {e}")
                                # Fallback to basic info
                                logging.info(f"Found live stream: {entry.get('title', 'Unknown')}")
                                self.last_stream_result = {
                                    'url': entry.get('url'),
                                    'title': entry.get('title', 'Live Stream'),
                                    'id': entry.get('id'),
                                    'is_live': True,
                                    'viewer_count': 0
                                }
                                return self.last_stream_result
        except Exception as e:
            logging.debug(f"Videos method failed: {e}")
        
        # Method 3: Use YouTube Data API
        try:
            search_response = self.youtube.search().list(
                channelId=SOURCE_CHANNEL_ID,
                part="id,snippet",
                type="video",
                eventType="live",
                maxResults=5
            ).execute()
            
            if search_response.get('items'):
                for item in search_response['items']:
                    video_id = item['id']['videoId']
                    # Get detailed video information
                    video_response = self.youtube.videos().list(
                        id=video_id,
                        part="liveStreamingDetails,snippet,statistics"
                    ).execute()
                    
                    if video_response.get('items'):
                        video = video_response['items'][0]
                        live_details = video.get('liveStreamingDetails', {})
                        
                        # Check if stream is currently live
                        if (live_details.get('actualStartTime') and 
                            not live_details.get('actualEndTime') and
                            live_details.get('concurrentViewers')):
                            
                            logging.info(f"Found live stream via API: {video['snippet']['title']}")
                            self.last_stream_result = {
                                'url': f"https://www.youtube.com/watch?v={video_id}",
                                'title': video['snippet']['title'],
                                'id': video_id,
                                'is_live': True,
                                'viewer_count': live_details.get('concurrentViewers', 0)
                            }
                            return self.last_stream_result
        except Exception as e:
            logging.debug(f"API method failed: {e}")
        
        # Method 4: Try to get current live stream from channel page
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,
                'force_json': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Get channel overview
                channel_url = f"https://www.youtube.com/channel/{SOURCE_CHANNEL_ID}"
                info = ydl.extract_info(channel_url, download=False)
                
                # Look for live streams in channel metadata
                if info.get('is_live') or info.get('live_status') == 'is_live':
                    logging.info(f"Channel is currently live: {info.get('title', 'Unknown')}")
                    self.last_stream_result = {
                        'url': info.get('webpage_url', channel_url),
                        'title': info.get('title', 'Live Stream'),
                        'id': info.get('id'),
                        'is_live': True,
                        'viewer_count': info.get('concurrent_view_count', 0)
                    }
                    return self.last_stream_result
                    
                # Check for related live streams
                if 'related_channels' in info or 'entries' in info:
                    related = info.get('related_channels', []) + info.get('entries', [])
                    for channel in related:
                        if channel.get('is_live') or channel.get('live_status') == 'is_live':
                            logging.info(f"Found related live stream: {channel.get('title', 'Unknown')}")
                            self.last_stream_result = {
                                'url': channel.get('url'),
                                'title': channel.get('title', 'Live Stream'),
                                'id': channel.get('id'),
                                'is_live': True,
                                'viewer_count': channel.get('concurrent_view_count', 0)
                            }
                            return self.last_stream_result
        except Exception as e:
            logging.debug(f"Channel page method failed: {e}")
        
        logging.info("No currently live streams found after trying all methods")
        self.last_stream_result = None
        return None
    
    def is_stream_still_live(self, stream_info):
        """Check if a previously detected stream is still live"""
        if not stream_info or not stream_info.get('is_live'):
            return False
            
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': False,  # Get full info to check live status
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(stream_info['url'], download=False)
                current_status = info.get('live_status')
                
                if current_status == 'is_live':
                    logging.debug(f"Stream {stream_info['title']} is still live")
                    return True
                else:
                    logging.info(f"Stream {stream_info['title']} is no longer live (status: {current_status})")
                    return False
                    
        except Exception as e:
            logging.warning(f"Error checking if stream is still live: {e}")
            # If we can't check, be conservative and assume it might have ended
            # But don't switch immediately - wait for the next full check
            return True  # Keep streaming unless we're sure it ended


    def check_currently_live_streams_old(self):
        """Check if the source channel has any currently live streams using yt-dlp"""
        # Cache results for 10 seconds to avoid excessive API calls
        current_time = time.time()
        if current_time - self.last_stream_check < 10:
            return self.last_stream_result
        
        self.last_stream_check = current_time
        logging.info(f"Checking for currently live streams on channel: {SOURCE_CHANNEL_ID}")
        
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,
                'force_json': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Get channel uploads playlist
                channel_url = f"https://www.youtube.com/channel/{SOURCE_CHANNEL_ID}/live"
                info = ydl.extract_info(channel_url, download=False)
                
                # Check if the channel is currently live
                if info.get('live_status') == 'is_live':
                    logging.info(f"Found live stream: {info.get('title', 'Unknown')}")
                    self.last_stream_result = {
                        'url': info.get('webpage_url'),
                        'title': info.get('title', 'Live Stream'),
                        'id': info.get('id'),
                        'is_live': True,
                        'viewer_count': info.get('concurrent_view_count', 0)
                    }
                    return self.last_stream_result
                
                # Alternative method: check channel videos for live streams
                channel_url = f"https://www.youtube.com/channel/{SOURCE_CHANNEL_ID}/videos"
                info = ydl.extract_info(channel_url, download=False)
                
                if 'entries' in info:
                    for entry in info['entries']:
                        if entry.get('live_status') == 'is_live':
                            stream_url = entry.get('url')
                            if stream_url:
                                logging.info(f"Found live stream: {entry.get('title', 'Unknown')}")
                                self.last_stream_result = {
                                    'url': stream_url,
                                    'title': entry.get('title', 'Live Stream'),
                                    'id': entry.get('id'),
                                    'is_live': True,
                                    'viewer_count': entry.get('concurrent_view_count', 0)
                                }
                                return self.last_stream_result
                
                logging.info("No currently live streams found")
                self.last_stream_result = None
                return None
                
        except Exception as e:
            logging.error(f"Error checking for live streams with yt-dlp: {e}")
            return self.check_currently_live_streams_api()  # Fallback to API

    def check_currently_live_streams_api(self):
        """Check for live streams using YouTube API (fallback method)"""
        logging.info("Using YouTube API to check for live streams")
        
        try:
            # Search for live broadcasts
            search_response = self.youtube.search().list(
                channelId=SOURCE_CHANNEL_ID,
                part="id,snippet",
                type="video",
                eventType="live",  # Currently live streams
                maxResults=10
            ).execute()
            
            if search_response.get('items'):
                for item in search_response['items']:
                    video_id = item['id']['videoId']
                    # Verify it's actually live
                    video_response = self.youtube.videos().list(
                        id=video_id,
                        part="liveStreamingDetails,snippet,statistics"
                    ).execute()
                    
                    if video_response.get('items'):
                        live_details = video_response['items'][0].get('liveStreamingDetails', {})
                        if live_details.get('actualStartTime') and not live_details.get('actualEndTime'):
                            logging.info(f"Found live stream via API: {item['snippet']['title']}")
                            self.last_stream_result = {
                                'url': f"https://www.youtube.com/watch?v={video_id}",
                                'title': item['snippet']['title'],
                                'id': video_id,
                                'is_live': True,
                                'viewer_count': video_response['items'][0].get('statistics', {}).get('viewCount', 0)
                            }
                            return self.last_stream_result
            
            logging.info("No live streams found via API")
            self.last_stream_result = None
            return None
            
        except Exception as e:
            logging.error(f"Error checking for live streams with API: {e}")
            return None

    def is_stream_still_live_old_sumthing(self, stream_info):
        """Check if a previously detected stream is still live"""
        if not stream_info or not stream_info.get('is_live'):
            return False
            
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(stream_info['url'], download=False)
                return info.get('live_status') == 'is_live'
                
        except Exception as e:
            logging.warning(f"Error checking if stream is still live: {e}")
            # If we can't check, assume it might have ended
            return False

    def start_live_stream(self, stream_url):
        """Start streaming a live stream using yt-dlp and ffmpeg"""
        logging.info(f"Starting live stream: {stream_url}")
        
        try:
            # Stop any existing ffmpeg process
            self.stop_ffmpeg()
            
            # Use yt-dlp to get the best stream URL
            ydl_opts = {
                'quiet': True,
                'format': 'best[height<=720]/best[height<=1080]/best',  # Prefer 720p or 1080p
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(stream_url, download=False)
                actual_url = info.get('url') or stream_url
                
                # If we didn't get a direct URL, use the original URL with yt-dlp format selection
                if not actual_url or 'youtube.com' in actual_url:
                    # Let yt-dlp handle the format selection internally
                    format_id = info.get('format_id', 'best')
                    actual_url = f"{stream_url} -f {format_id}"
            
            # Build the FFmpeg command
            if ' -f ' in actual_url:
                # If yt-dlp format spec is included, use it as input to yt-dlp then pipe to ffmpeg
                cmd = [
                    'yt-dlp',
                    '-f', 'best[height<=720]',
                    '-o', '-',  # Output to stdout
                    stream_url,
                    '|',
                    'ffmpeg',
                    '-v', 'info',
                    '-i', '-',  # Read from stdin
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-b:v', '3000k',
                    '-maxrate', '3000k',
                    '-bufsize', '6000k',
                    '-pix_fmt', 'yuv420p',
                    '-g', str(INTRA),
                    '-c:a', 'aac',
                    '-b:a', '128k',
                    '-f', 'flv',
                    f"rtmp://a.rtmp.youtube.com/live2/{STREAM_KEY}"
                ]
                
                # Use shell=True for piping
                self.ffmpeg_process = subprocess.Popen(
                    ' '.join(cmd),
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1
                )
            else:
                # Direct URL available
                cmd = [
                    'ffmpeg',
                    '-v', 'info',
                    '-i', actual_url,
                    '-c:v', 'libx264',
                    '-preset', 'veryfast',
                    '-b:v', '3000k',
                    '-maxrate', '3000k',
                    '-bufsize', '6000k',
                    '-pix_fmt', 'yuv420p',
                    '-g', str(INTRA),
                    '-c:a', 'aac',
                    '-b:a', '128k',
                    '-f', 'flv',
                    f"rtmp://a.rtmp.youtube.com/live2/{STREAM_KEY}"
                ]
                
                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1
                )
            
            self.stream_start_time = time.time()
            
            # Start thread to log FFmpeg output
            threading.Thread(
                target=self.log_ffmpeg_output,
                daemon=True
            ).start()
            
            logging.info("Live stream started successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error starting live stream: {e}")
            return False



    def start_live_stream_old(self, stream_url):
        """Start streaming a live stream using yt-dlp and ffmpeg"""
        logging.info(f"Starting live stream: {stream_url}")
        
        try:
            # Stop any existing ffmpeg process
            self.stop_ffmpeg()
            
            # Use yt-dlp to get the best stream URL
            ydl_opts = {
                'quiet': True,
                'format': 'best[height<=720]',  # Limit to 720p for stability
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(stream_url, download=False)
                stream_url = info['url']  # Get the direct stream URL
            
            cmd = [
                'ffmpeg',
                '-v', 'info',
                '-i', stream_url,  # Stream directly from the live URL
                '-c:v', 'libx264',
                '-preset', 'veryfast',
                '-b:v', '3000k',
                '-maxrate', '3000k',
                '-bufsize', '6000k',
                '-pix_fmt', 'yuv420p',
                '-g', str(INTRA),
                '-c:a', 'aac',
                '-b:a', '128k',
                '-f', 'flv',
                f"rtmp://a.rtmp.youtube.com/live2/{STREAM_KEY}"
            ]
            
            logging.info("Starting FFmpeg for live stream")
            self.ffmpeg_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            self.stream_start_time = time.time()
            
            # Start thread to log FFmpeg output
            threading.Thread(
                target=self.log_ffmpeg_output,
                daemon=True
            ).start()
            
            logging.info("Live stream started successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error starting live stream: {e}")
            return False

    def start_comeback_stream(self):
        """Stream the comeback image with no audio"""
        logging.info("Starting comeback image stream")
        
        try:
            # Stop any existing ffmpeg process
            self.stop_ffmpeg()
            
            cmd = [
                'ffmpeg',
                '-v', 'info',
                '-re',  # Read at native frame rate
                '-loop', '1',  # Loop the single image
                '-i', COMEBACK_IMAGE,
                '-f', 'lavfi',
                '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100',
                '-c:v', 'libx264',
                '-preset', 'veryfast',
                '-b:v', '1500k',
                '-pix_fmt', 'yuv420p',
                '-r', str(FRAMERATE),
                '-g', str(INTRA),
                '-c:a', 'aac',
                '-b:a', '64k',
                '-shortest',  # End when video ends (but it loops forever)
                '-f', 'flv',
                f"rtmp://a.rtmp.youtube.com/live2/{STREAM_KEY}"
            ]
            
            logging.info("Starting FFmpeg for comeback image")
            self.ffmpeg_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            self.stream_start_time = time.time()
            
            # Start thread to log FFmpeg output
            threading.Thread(
                target=self.log_ffmpeg_output,
                daemon=True
            ).start()
            
            logging.info("Comeback image stream started successfully")
            return True
            
        except Exception as e:
            logging.error(f"Error starting comeback stream: {e}")
            return False

    def log_ffmpeg_output(self):
        """Log FFmpeg output in real-time with better error filtering"""
        logging.info("Starting FFmpeg output logger")
        error_count = 0
        max_errors_before_restart = 10  # Allow some transient errors
        
        try:
            for line in iter(self.ffmpeg_process.stdout.readline, ''):
                if line.strip():
                    line_lower = line.lower()
                    
                    # Check for specific error patterns that indicate real problems
                    if any(error_indicator in line_lower for error_indicator in 
                           ['error: unable to open resource', 'connection failed', 
                            'server returned 4', 'invalid data found', 
                            'http error 4', 'http error 5', 'stream ended', 
                            'end of file', 'signal 15']):
                        logging.error(f"FFmpeg Critical Error: {line.strip()}")
                        error_count += 1
                        
                        # Only restart if we see multiple critical errors in a short time
                        if error_count >= max_errors_before_restart:
                            logging.error("Too many critical errors, stream may need restart")
                            break
                    elif 'error' in line_lower or 'failed' in line_lower:
                        # Log non-critical errors as warnings
                        logging.warning(f"FFmpeg Warning: {line.strip()}")
                    else:
                        # Normal debug info
                        logging.debug(f"FFmpeg: {line.strip()}")
                        
        except Exception as e:
            logging.error(f"Error in FFmpeg logger: {e}")
        logging.info("FFmpeg output logger stopped")

    def log_ffmpeg_output_old(self):
        """Log FFmpeg output in real-time"""
        logging.info("Starting FFmpeg output logger")
        try:
            for line in iter(self.ffmpeg_process.stdout.readline, ''):
                if line.strip():
                    # Check for specific error patterns that indicate stream issues
                    if 'error' in line.lower() or 'failed' in line.lower():
                        logging.error(f"FFmpeg Error: {line.strip()}")
                    else:
                        logging.debug(f"FFmpeg: {line.strip()}")
        except Exception as e:
            logging.error(f"Error in FFmpeg logger: {e}")
        logging.info("FFmpeg process ended")

    def stop_ffmpeg(self):
        """Stop the current FFmpeg process"""
        if self.ffmpeg_process:
            logging.info("Stopping FFmpeg process")
            try:
                self.ffmpeg_process.terminate()
                self.ffmpeg_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("FFmpeg didn't stop gracefully, killing it")
                self.ffmpeg_process.kill()
            except Exception as e:
                logging.error(f"Error stopping FFmpeg: {e}")
            finally:
                self.ffmpeg_process = None
                self.stream_start_time = None

    def monitor_stream_health_old_old_old(self):
        """Monitor the health of the current stream with better error tolerance"""
        if not self.ffmpeg_process:
            logging.warning("No FFmpeg process running")
            return False
            
        # Check if FFmpeg process is still running
        if self.ffmpeg_process.poll() is not None:
            logging.warning("FFmpeg process has stopped unexpectedly")
            return False
            
        # Only check YouTube live status if we're supposed to be streaming a live stream
        if self.current_stream and self.current_stream.get('is_live'):
            # Don't check YouTube status every time - only every 60 seconds to avoid API limits
            current_time = time.time()
            if (not hasattr(self, 'last_live_check') or 
                current_time - self.last_live_check > 60):  # Increased to 60 seconds
                
                self.last_live_check = current_time
                if not self.is_stream_still_live(self.current_stream):
                    logging.info("Live stream has ended (health check)")
                    return False
                    
        return True
    

    def monitor_stream_health_old_old(self):
        """Monitor the health of the current stream"""
        if not self.ffmpeg_process:
            return False
            
        # Check if FFmpeg process is still running
        if self.ffmpeg_process.poll() is not None:
            logging.warning("FFmpeg process has stopped unexpectedly")
            return False
            
        # Only check YouTube live status if we're supposed to be streaming a live stream
        if self.current_stream and self.current_stream.get('is_live'):
            # Don't check YouTube status every time - only every 30 seconds to avoid API limits
            current_time = time.time()
            if (not hasattr(self, 'last_live_check') or 
                current_time - self.last_live_check > 30):
                
                self.last_live_check = current_time
                if not self.is_stream_still_live(self.current_stream):
                    logging.info("Live stream has ended (health check)")
                    return False
                    
        return True

    def monitor_stream_health_old(self):
        """Monitor the health of the current stream"""
        if not self.ffmpeg_process:
            return False
            
        # Check if FFmpeg process is still running
        if self.ffmpeg_process.poll() is not None:
            logging.warning("FFmpeg process has stopped unexpectedly")
            return False
            
        # If we're streaming a live stream, check if it's still live
        if self.current_stream and self.current_stream.get('is_live'):
            if not self.is_stream_still_live(self.current_stream):
                logging.info("Live stream has ended")
                return False
                
        return True

    def should_switch_streams(self, new_stream):
        """Determine if we should switch from current stream to new stream"""
        current_is_live = self.current_stream and self.current_stream.get('is_live')
        new_is_live = new_stream and new_stream.get('is_live')
        
        # If we're showing comeback image and a live stream starts
        if not current_is_live and new_is_live:
            logging.info("Switching from comeback image to live stream")
            return True
            
        # If we're streaming a live stream and it ends
        if current_is_live and not new_is_live:
            logging.info("Switching from ended live stream to comeback image")
            return True
            
        # If a new live stream starts while we're streaming a different one
        if (current_is_live and new_is_live and 
            self.current_stream.get('id') != new_stream.get('id')):
            logging.info("New live stream detected, switching to it")
            return True
            
        # If FFmpeg process is not running but it should be
        if (self.current_stream and not self.ffmpeg_process):
            logging.info("FFmpeg process not running, restarting stream")
            return True
            
        return False

    def should_switch_streams_old_old(self, new_stream):
        """Determine if we should switch from current stream to new stream"""
        current_is_live = self.current_stream and self.current_stream.get('is_live')
        new_is_live = new_stream and new_stream.get('is_live')
        
        # If we're showing comeback image and a live stream starts
        if not current_is_live and new_is_live:
            logging.info("Switching from comeback image to live stream")
            return True
            
        # If we're streaming a live stream and it ends
        if current_is_live and not new_is_live:
            logging.info("Switching from ended live stream to comeback image")
            return True
            
        # If a new live stream starts while we're streaming a different one
        if (current_is_live and new_is_live and 
            self.current_stream.get('id') != new_stream.get('id')):
            logging.info("New live stream detected, switching to it")
            return True
            
        # If FFmpeg process is not running but it should be
        if (current_is_live or not current_is_live) and not self.ffmpeg_process:
            logging.info("FFmpeg process not running, restarting stream")
            return True
            
        return False

    def should_switch_streams_old(self, new_stream):
        """Determine if we should switch from current stream to new stream"""
        # If we're showing comeback image and a live stream starts
        if not self.current_stream and new_stream:
            logging.info("Switching from comeback image to live stream")
            return True
            
        # If we're streaming a live stream and it ends
        if self.current_stream and not new_stream:
            logging.info("Switching from ended live stream to comeback image")
            return True
            
        # If a new live stream starts while we're streaming a different one
        if (self.current_stream and new_stream and 
            self.current_stream.get('id') != new_stream.get('id')):
            logging.info("New live stream detected, switching to it")
            return True
            
        return False

    def is_stream_still_live(self, stream_info):
        """Check if a previously detected stream is still live using yt-dlp (no API calls)"""
        if not stream_info or not stream_info.get('is_live'):
            return False
            
        try:
            ydl_opts = {
                'quiet': True,
                'extract_flat': True,  # Fast metadata-only check
                'no_warnings': True,
                'socket_timeout': 10,  # 10 second timeout
                'extractor_args': {'youtube': {'skip': ['dash', 'hls']}},  # Skip heavy processing
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(stream_info['url'], download=False, process=False)
                current_status = info.get('live_status')
                
                if current_status == 'is_live':
                    logging.debug(f"Stream {stream_info['title']} is still live (yt-dlp check)")
                    return True
                else:
                    logging.info(f"Stream {stream_info['title']} is no longer live (status: {current_status})")
                    return False
                    
        except Exception as e:
            # If we can't check, assume the stream might still be live to avoid unnecessary restarts
            logging.debug(f"yt-dlp check failed (stream might still be live): {e}")
            return True  # Be conservative - assume it's still live unless we're sure it ended
    
    def monitor_stream_health(self):
        """Monitor the health of the current stream using efficient methods"""
        if not self.ffmpeg_process:
            logging.warning("No FFmpeg process running")
            return False
            
        # Check if FFmpeg process is still running (primary health check)
        if self.ffmpeg_process.poll() is not None:
            logging.warning("FFmpeg process has stopped unexpectedly")
            return False
            
        # For live streams, do occasional yt-dlp checks (no API calls)
        if self.current_stream and self.current_stream.get('is_live'):
            current_time = time.time()
            
            # Only check every 5 minutes to avoid excessive network calls
            if (not hasattr(self, 'last_ytdlp_check') or 
                current_time - self.last_ytdlp_check > 300):  # 5 minutes
                
                self.last_ytdlp_check = current_time
                if not self.is_stream_still_live(self.current_stream):
                    logging.info("Live stream has ended (yt-dlp check)")
                    return False
                    
        return True
    
    def check_stream_quality(self):
        """Alternative: Check stream quality by testing the actual stream URL"""
        if not self.current_stream or not self.ffmpeg_process:
            return False
            
        try:
            # Test if we can get a small segment of the stream
            ydl_opts = {
                'quiet': True,
                'format': 'worst[height<=360]',  # Lowest quality for fast check
                'socket_timeout': 15,
                'extract_flat': False,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Try to extract stream info (this will fail if stream is dead)
                info = ydl.extract_info(self.current_stream['url'], download=False)
                
                # If we get here, the stream is accessible
                logging.debug("Stream quality check passed")
                return True
                
        except Exception as e:
            logging.warning(f"Stream quality check failed: {e}")
            return False


    def run(self):
        """Main streaming loop with efficient health checking"""
        logging.info("Starting main streaming loop")
        
        self.youtube = self.authenticate()
        if not self.youtube:
            logging.error("Failed to authenticate with YouTube API")
            return
    
        current_mode = None
        consecutive_health_checks = 0
        last_stream_check = 0
        
        while not self.stop_event.is_set():
            try:
                # Reduce logging frequency when streaming live
                should_log = not (self.current_stream and self.current_stream.get('is_live'))
                should_log = should_log or (consecutive_health_checks % 30 == 0)  # Log every ~5 minutes
                
                if should_log:
                    logging.info("\n" + "="*50)
                    logging.info("Stream status check")
                    if self.current_stream:
                        uptime = time.time() - self.stream_start_time if self.stream_start_time else 0
                        logging.info(f"Current: {self.current_stream.get('title', 'Unknown')} (uptime: {uptime//60:.0f}m)")
                    else:
                        logging.info("Current: Comeback image")
                    logging.info("="*50)
    
                # Only check for NEW live streams if we're not currently streaming one
                new_stream = None
                current_time = time.time()
                
                if not self.current_stream or not self.current_stream.get('is_live'):
                    # We're showing comeback image - check for new streams more frequently
                    if current_time - last_stream_check > 30:  # Every 30 seconds
                        logging.info("Checking for new live streams...")
                        new_stream = self.check_currently_live_streams()
                        last_stream_check = current_time
                    else:
                        new_stream = None
                else:
                    # We're streaming a live stream - just monitor health efficiently
                    if self.monitor_stream_health():
                        consecutive_health_checks += 1
                        new_stream = self.current_stream  # Keep current stream
                    else:
                        logging.info("Stream health check failed")
                        new_stream = None  # Force re-evaluation
    
                # Check if we need to switch streams
                if self.should_switch_streams(new_stream):
                    logging.info("Stream switch required")
                    consecutive_health_checks = 0
                    last_stream_check = current_time
                    
                    if new_stream:
                        # Start new live stream
                        if self.start_live_stream(new_stream['url']):
                            self.current_stream = new_stream
                            current_mode = 'live'
                            logging.info(f"Now streaming: {new_stream['title']}")
                        else:
                            logging.error("Failed to start live stream")
                            # Fall back to comeback image
                            if self.start_comeback_stream():
                                self.current_stream = None
                                current_mode = 'comeback'
                    else:
                        # Switch to comeback image
                        if self.start_comeback_stream():
                            self.current_stream = None
                            current_mode = 'comeback'
                            logging.info("Now showing comeback image")
                
                else:
                    # No switch needed - stream is healthy
                    consecutive_health_checks += 1
                    
                    # Occasional status logging
                    if consecutive_health_checks % 36 == 0:  # Every ~6 minutes
                        if self.current_stream and self.current_stream.get('is_live'):
                            uptime = time.time() - self.stream_start_time
                            logging.info(f"Stream stable: {self.current_stream['title']} (uptime: {uptime//60:.0f}m)")
                        else:
                            logging.debug("Comeback image stream stable")
    
                # Adjust wait time based on current mode
                if self.current_stream and self.current_stream.get('is_live'):
                    # When streaming live, quick health checks but infrequent yt-dlp checks
                    wait_time = 10  # Quick process checks every 10 seconds
                else:
                    # When showing comeback image, check for new streams periodically
                    wait_time = 10  # But still quick checks
                
                # Efficient waiting
                for i in range(wait_time):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
    
            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
                logging.info("Waiting 30 seconds before continuing")
                time.sleep(30)
        
    def run_old(self):
        """Main streaming loop"""
        logging.info("Starting main streaming loop")
        
        self.youtube = self.authenticate()
        if not self.youtube:
            logging.error("Failed to authenticate with YouTube API")
            return
    
        current_mode = None  # 'live' or 'comeback'
        
        while not self.stop_event.is_set():
            try:
                logging.info("\n" + "="*50)
                logging.info("Checking stream status...")
                if self.current_stream:
                    logging.info(f"Current: {self.current_stream.get('title', 'Unknown')}")
                    if self.stream_start_time:
                        uptime = time.time() - self.stream_start_time
                        logging.info(f"Stream uptime: {uptime//3600:.0f}h {uptime%3600//60:.0f}m")
                logging.info("="*50)
    
                # Only check for new live streams if we're NOT currently streaming a live stream
                new_stream = None
                if not self.current_stream or not self.current_stream.get('is_live'):
                    # We're either streaming the comeback image or no stream at all
                    logging.info("Checking for new live streams...")
                    new_stream = self.check_currently_live_streams()
                else:
                    # We're already streaming a live stream, just verify it's still live
                    logging.debug("Already streaming a live stream, checking if it's still active")
                    if not self.is_stream_still_live(self.current_stream):
                        logging.info("Current live stream has ended")
                        new_stream = None  # Force switch to comeback image
                    else:
                        logging.debug("Current live stream is still active")
                        new_stream = self.current_stream  # Keep streaming the same one
    
                # Check if we need to switch streams
                if self.should_switch_streams(new_stream) or not self.monitor_stream_health():
                    logging.info("Stream switch required or health check failed")
                    
                    if new_stream:
                        # Start new live stream
                        if self.start_live_stream(new_stream['url']):
                            self.current_stream = new_stream
                            current_mode = 'live'
                            logging.info(f"Now streaming: {new_stream['title']}")
                        else:
                            logging.error("Failed to start live stream")
                            # Fall back to comeback image
                            if self.start_comeback_stream():
                                self.current_stream = None
                                current_mode = 'comeback'
                    else:
                        # Switch to comeback image
                        if self.start_comeback_stream():
                            self.current_stream = None
                            current_mode = 'comeback'
                            logging.info("Now showing comeback image")
                
                else:
                    # No switch needed, just log status
                    if self.current_stream and self.current_stream.get('is_live'):
                        logging.debug(f"Continuing current live stream: {self.current_stream['title']}")
                    else:
                        logging.debug("Continuing comeback image stream")
    
                # Adjust wait time based on current mode
                if self.current_stream and self.current_stream.get('is_live'):
                    # When streaming a live stream, only check health frequently but don't search for new streams
                    wait_time = 10  # Check health every 10 seconds
                    logging.info(f"Streaming live content, waiting {wait_time} seconds for health check")
                else:
                    # When streaming comeback image, check for new streams more frequently
                    wait_time = CHECK_INTERVAL  # 30 seconds
                    logging.info(f"Waiting {wait_time} seconds before checking for new streams")
                
                # Wait with periodic health checks
                for i in range(wait_time):
                    if self.stop_event.is_set():
                        break
                        
                    # Only check stream health if we're streaming a live stream
                    if self.current_stream and self.current_stream.get('is_live'):
                        if i % 5 == 0:  # Check health every 5 seconds when streaming live
                            if not self.monitor_stream_health():
                                logging.warning("Stream health check failed during wait")
                                break
                    time.sleep(1)
    
            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
                logging.info("Waiting 30 seconds before continuing")
                time.sleep(30)

    def run_old(self):
        """Main streaming loop"""
        logging.info("Starting main streaming loop")
        
        self.youtube = self.authenticate()
        if not self.youtube:
            logging.error("Failed to authenticate with YouTube API")
            return

        current_mode = None  # 'live' or 'comeback'
        
        while not self.stop_event.is_set():
            try:
                logging.info("\n" + "="*50)
                logging.info("Checking stream status...")
                if self.current_stream:
                    logging.info(f"Current: {self.current_stream.get('title', 'Unknown')}")
                    if self.stream_start_time:
                        uptime = time.time() - self.stream_start_time
                        logging.info(f"Stream uptime: {uptime//3600:.0f}h {uptime%3600//60:.0f}m")
                logging.info("="*50)

                # Check for currently live streams
                new_stream = self.check_currently_live_streams()
                
                # Check if we need to switch streams
                if self.should_switch_streams(new_stream) or not self.monitor_stream_health():
                    logging.info("Stream switch required or health check failed")
                    
                    if new_stream:
                        # Start new live stream
                        if self.start_live_stream(new_stream['url']):
                            self.current_stream = new_stream
                            current_mode = 'live'
                            logging.info(f"Now streaming: {new_stream['title']}")
                        else:
                            logging.error("Failed to start live stream")
                            # Fall back to comeback image
                            if self.start_comeback_stream():
                                self.current_stream = None
                                current_mode = 'comeback'
                    else:
                        # Switch to comeback image
                        if self.start_comeback_stream():
                            self.current_stream = None
                            current_mode = 'comeback'
                            logging.info("Now showing comeback image")
                
                else:
                    # No switch needed, just log status
                    if new_stream and self.current_stream:
                        logging.debug(f"Continuing current live stream: {self.current_stream['title']}")
                    elif not new_stream and not self.current_stream:
                        logging.debug("Continuing comeback image stream")
                    else:
                        logging.debug("Stream status unchanged")

                # Wait before checking again
                wait_time = CHECK_INTERVAL
                logging.info(f"Waiting {wait_time} seconds before next check")
                
                for i in range(wait_time):
                    if self.stop_event.is_set():
                        break
                    # Check stream health more frequently during live streams
                    if i % 10 == 0 and not self.monitor_stream_health():
                        logging.warning("Stream health check failed during wait")
                        break
                    time.sleep(1)

            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
                logging.info("Waiting 30 seconds before continuing")
                time.sleep(30)

    def stop(self):
        """Graceful shutdown"""
        logging.info("Initiating graceful shutdown...")
        self.stop_event.set()
        self.stop_ffmpeg()
        logging.info("LiveStreamManager shutdown complete")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('live_stream_manager.log')
        ]
    )
    logging.info("=== Starting YouTube Live Stream Manager ===")

    manager = LiveStreamManager()
    try:
        manager.run()
    except KeyboardInterrupt:
        logging.info("\nReceived keyboard interrupt, shutting down...")
        manager.stop()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        manager.stop()
    finally:
        logging.info("=== YouTube Live Stream Manager Stopped ===")
