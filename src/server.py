import os
import subprocess
import io
import logging
from flask import Flask, request, jsonify
from pathlib import Path
import tempfile

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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


        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        frame_num = 0
        frame_data = b""

        while True:
            chunk = process.stdout.read(4096)

            if not chunk:
                break

            frame_data += chunk


            while True:
                end_marker_pos = frame_data.find(b'\xff\xd9')

                if end_marker_pos == -1:
                    break

                complete_frame = frame_data[:end_marker_pos + 2]

                frame_callback(complete_frame, frame_num)
                frame_num += 1

                frame_data = frame_data[end_marker_pos + 2:]

        process.wait()

        if process.returncode != 0:
            error = process.stderr.read().decode()
            logger.error(f"FFmpeg streaming error: {error}")
            raise Exception(f"FFmpeg error: {error}")

        logger.info(f"Streaming extraction complete. Total frames: {frame_num}")
        return frame_num

extractor = FrameExtractor()

@app.route('/upload', methods=['POST'])
def upload_video():
    """
    Handle video upload and extract frames
    """
    logger.info("Received video upload request")

    if 'video' not in request.files:
        logger.warning("No video file provided in request")
        return jsonify({"error": "No video file provided"}), 400

    video_file = request.files['video']

    if video_file.filename == '':
        logger.warning("Empty filename in request")
        return jsonify({"error": "No selected file"}), 400

    logger.info(f"Processing video file: {video_file.filename}")

    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video:
        video_file.save(temp_video.name)
        temp_video_path = temp_video.name

    logger.info(f"Video saved to temporary file: {temp_video_path}")

    try:
        def frame_callback(frame_data, frame_num):
            """
            This gets called for each extracted frame
            frame_data: raw JPEG bytes
            frame_num: frame number (0, 1, 2, ...)
            """
            logger.debug(f"Extracted frame {frame_num}, size: {len(frame_data)} bytes")

            frame_path = os.path.join(extractor.output_dir, f"frame_{frame_num:04d}.jpg")
            with open(frame_path, 'wb') as f:
                f.write(frame_data)


        # Extract frames
        num_frames = extractor.extract_frames_streaming(temp_video_path, frame_callback)

        logger.info(f"Successfully extracted {num_frames} frames from {video_file.filename}")

        return jsonify({
            "message": "Frames extracted successfully",
            "num_frames": num_frames
        }), 200

    except Exception as e:
        logger.error(f"Error processing video: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

    finally:
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
            logger.debug(f"Cleaned up temporary file: {temp_video_path}")

@app.route('/health', methods=['GET'])
def health():
    logger.debug("Health check requested")
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Starting Video Frame Extractor Server")
    logger.info("Server will listen on: http://0.0.0.0:8080")
    logger.info("Upload video with: curl -F 'video=@your_video.mp4' http://localhost:8080/upload")
    logger.info("=" * 60)
    app.run(host='0.0.0.0', port=8080, debug=True)
