import os
import subprocess
import io
import logging
import time
import asyncio
import threading
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pathlib import Path
import tempfile
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("=" * 60)
    logger.info("Starting Video Frame Extractor Server")
    logger.info("Server will listen on: http://0.0.0.0:8080")
    logger.info(f"Frames will be saved to: {os.path.abspath(extractor.output_dir)}")
    logger.info("Upload video with: curl -F 'video=@your_video.mp4' http://localhost:8080/upload")
    logger.info("=" * 60)
    yield
    # Shutdown
    logger.info("Shutting down server")

app = FastAPI(title="Video Frame Extractor", lifespan=lifespan)

class FrameExtractor:
    def __init__(self, output_dir="./frames"):
        self.output_dir = output_dir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"FrameExtractor initialized with output directory: {output_dir}")
    
    def extract_frames_to_files(self, video_path, frame_callback=None):
        """
        Extract frames and save to files
        """
        logger.info(f"Starting frame extraction from: {video_path}")
        output_pattern = os.path.join(self.output_dir, "frame_%04d.jpg")

        cmd = [
            "ffmpeg",
            "-i", video_path,           # Input file
            "-vf", "fps=2",             # Extract 2 frames per second
            "-q:v", "2",                # Quality (2 is high quality)
            output_pattern              # Output pattern
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)

            frames = sorted(Path(self.output_dir).glob("frame_*.jpg"))
            logger.info(f"Extracted {len(frames)} frames to files")

            if frame_callback:
                for frame_path in frames:
                    frame_callback(str(frame_path))

            return len(frames)

        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg error: {e.stderr.decode()}")
            raise
    
    def extract_frames_streaming(self, video_path, frame_callback):
        """
        Extract frames in real-time and get raw JPEG bytes
        This is better for sending frames to another server immediately
        """
        start_time = time.time()
        logger.info(f"Starting streaming frame extraction from: {video_path}")
        cmd = [
            "ffmpeg",
            "-i", video_path,
            "-vf", "fps=2",             # 2 frames per second
            "-f", "image2pipe",         # Output to pipe
            "-vcodec", "mjpeg",         # MJPEG codec
            "-q:v", "2",                # High quality
            "-"                         # Output to stdout
        ]

        ffmpeg_start_time = time.time()
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        frame_num = 0
        frame_data = b""
        frame_times = []
        first_frame_time = None
        last_frame_time = None

        while True:
            chunk = process.stdout.read(4096)

            if not chunk:
                break

            frame_data += chunk


            while True:
                end_marker_pos = frame_data.find(b'\xff\xd9')

                if end_marker_pos == -1:
                    break

                frame_start = time.time()
                complete_frame = frame_data[:end_marker_pos + 2]

                # Track first frame time
                if frame_num == 0:
                    first_frame_time = time.time() - start_time
                    logger.info(f"First frame available after: {first_frame_time*1000:.2f}ms")

                frame_callback(complete_frame, frame_num)
                frame_end = time.time()
                frame_times.append(frame_end - frame_start)

                # Track last frame time
                last_frame_time = time.time() - start_time

                frame_num += 1

                frame_data = frame_data[end_marker_pos + 2:]

        process.wait()

        if process.returncode != 0:
            error = process.stderr.read().decode()
            logger.error(f"FFmpeg streaming error: {error}")
            raise Exception(f"FFmpeg error: {error}")

        total_time = time.time() - start_time
        ffmpeg_time = time.time() - ffmpeg_start_time
        avg_frame_time = sum(frame_times) / len(frame_times) if frame_times else 0

        metrics = {
            'num_frames': frame_num,
            'total_time_s': round(total_time, 3),
            'ffmpeg_time_s': round(ffmpeg_time, 3),
            'first_frame_time_ms': round(first_frame_time * 1000, 2) if first_frame_time else None,
            'last_frame_time_ms': round(last_frame_time * 1000, 2) if last_frame_time else None,
            'latency_first_to_last_ms': round((last_frame_time - first_frame_time) * 1000, 2) if (first_frame_time and last_frame_time) else None,
            'avg_frame_processing_ms': round(avg_frame_time * 1000, 2),
            'throughput_fps': round(frame_num / total_time, 2) if total_time > 0 else 0,
            'throughput_frames_per_min': round((frame_num / total_time) * 60, 2) if total_time > 0 else 0
        }

        import json
        logger.info(f"Extraction metrics: {json.dumps(metrics, indent=2)}")

        return metrics

extractor = FrameExtractor()

@app.post('/upload')
async def upload_video(video: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """
    Handle video upload and extract frames with streaming.
    Returns immediately after first frame is extracted.
    """
    request_start_time = time.time()
    logger.info("Received video upload request")

    if not video.filename:
        logger.warning("Empty filename in request")
        raise HTTPException(status_code=400, detail="No selected file")

    logger.info(f"Processing video file: {video.filename}")

    # Create temp file and start streaming upload + extraction concurrently
    temp_video_path = tempfile.mktemp(suffix='.mp4')
    upload_start = time.time()

    # Shared state between upload and extraction
    first_frame_event = threading.Event()
    first_frame_data = {'time': None, 'path': None, 'error': None}
    extraction_complete = threading.Event()
    extraction_stats = {'num_frames': 0, 'metrics': None, 'io_times': []}

    file_size_bytes = 0

    async def stream_upload_to_file():
        """Stream video upload to disk chunk by chunk"""
        nonlocal file_size_bytes
        try:
            with open(temp_video_path, 'wb') as f:
                while chunk := await video.read(65536):  # 64KB chunks
                    f.write(chunk)
                    file_size_bytes += len(chunk)
            logger.info(f"Upload complete: {file_size_bytes / (1024*1024):.2f} MB")
        except Exception as e:
            logger.error(f"Upload error: {e}")
            first_frame_data['error'] = str(e)
            first_frame_event.set()

    def extract_frames_background():
        """Run frame extraction in background thread"""
        io_times = []

        def frame_callback(frame_data, frame_num):
            io_start = time.time()
            frame_path = os.path.join(extractor.output_dir, f"frame_{frame_num:04d}.jpg")
            with open(frame_path, 'wb') as f:
                f.write(frame_data)
            io_times.append(time.time() - io_start)

            # Signal first frame is ready
            if frame_num == 0:
                first_frame_data['time'] = time.time()
                first_frame_data['path'] = frame_path
                first_frame_event.set()
                logger.info(f"First frame ready at: {frame_path}")

        try:
            # Wait for file to have some data before starting extraction
            time.sleep(0.05)  # Small delay to ensure file has started writing

            metrics = extractor.extract_frames_streaming(temp_video_path, frame_callback)
            extraction_stats['num_frames'] = metrics['num_frames']
            extraction_stats['metrics'] = metrics
            extraction_stats['io_times'] = io_times

            # Log comprehensive metrics including I/O timing
            avg_io_time = sum(io_times) / len(io_times) if io_times else 0
            import json
            complete_metrics = {
                **metrics,
                'avg_io_time_ms': round(avg_io_time * 1000, 2),
                'total_io_time_ms': round(sum(io_times) * 1000, 2)
            }
            logger.info(f"Background extraction complete - Full metrics: {json.dumps(complete_metrics, indent=2)}")

            extraction_complete.set()
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            first_frame_data['error'] = str(e)
            first_frame_event.set()

    # Start upload and extraction concurrently
    extraction_thread = threading.Thread(target=extract_frames_background, daemon=True)
    extraction_thread.start()

    upload_task = asyncio.create_task(stream_upload_to_file())

    # Wait for first frame to be ready (or error)
    def wait_for_first_frame():
        first_frame_event.wait(timeout=30)  # 30 second timeout

    await asyncio.get_event_loop().run_in_executor(None, wait_for_first_frame)

    # Ensure upload is complete
    await upload_task
    upload_time = time.time() - upload_start

    # Check for errors
    if first_frame_data['error']:
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise HTTPException(status_code=500, detail=first_frame_data['error'])

    if not first_frame_event.is_set():
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        raise HTTPException(status_code=500, detail="Timeout waiting for first frame")

    # Calculate timings
    file_size_mb = file_size_bytes / (1024 * 1024)
    time_to_first_frame = first_frame_data['time'] - request_start_time
    response_time = time.time() - request_start_time

    logger.info(f"Returning response after first frame: {response_time*1000:.2f}ms")
    logger.info(f"Time to first frame: {time_to_first_frame*1000:.2f}ms")

    # Schedule cleanup after all frames are extracted
    def cleanup_when_done():
        extraction_complete.wait(timeout=60)
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
            logger.debug(f"Cleaned up temporary file: {temp_video_path}")

    background_tasks.add_task(cleanup_when_done)

    # Get absolute path to frames directory
    frames_dir = os.path.abspath(extractor.output_dir)

    return {
        "message": "First frame extracted, remaining frames processing in background",
        "first_frame_path": first_frame_data['path'],
        "frames_location": frames_dir
        }

@app.get('/health')
async def health():
    logger.debug("Health check requested")
    return {"status": "healthy"}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080)
