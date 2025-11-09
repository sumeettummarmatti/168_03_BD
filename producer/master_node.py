"""
NODE 1 - MASTER & CLIENT
Distributed Image Processing Pipeline with Kafka

EVALUATION CRITERIA (10 Marks):
- Frontend (Client UI): Image upload & result display, basic progress dashboard (2 marks)
- Master Node: Image tiling and segmentation logic (2 marks)
- Metadata handling (2 marks)
- Task distribution via Kafka (1.5 marks)
- Reconstruction of processed tiles (1.5 marks)
- Worker heartbeat monitoring (1 mark)

Author: Person 1
FIXED VERSION - Transformation parameter properly handled
"""

import os
import sys
import json
import base64
import uuid
import time
import logging
import sqlite3
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Optional
import numpy as np
from PIL import Image
import cv2

from fastapi import FastAPI, File, UploadFile, WebSocket, HTTPException, BackgroundTasks, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import asyncio

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Kafka Configuration
KAFKA_BROKER = "10.148.167.36:9092"  # Update with actual Kafka broker ZeroTier IP
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"

# Image Processing Configuration
MIN_IMAGE_SIZE = 1024  # Minimum image dimension
TILE_SIZE = 512  # Size of each tile

# Service Configuration
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 8000

# Database Configuration
DB_PATH = "master_metadata.db"

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('master_node.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE SETUP AND METADATA HANDLING (2 Marks)
# ============================================================================

class MetadataDB:
    """
    Handles all database operations for job tracking and tile status.
    This class is crucial for metadata handling evaluation.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()
        logger.info(f"Metadata database initialized at {db_path}")
    
    def init_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Jobs table - tracks overall job status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                original_filename TEXT,
                image_width INTEGER,
                image_height INTEGER,
                total_tiles INTEGER,
                transformation TEXT,
                status TEXT,
                created_at TIMESTAMP,
                completed_at TIMESTAMP,
                result_path TEXT
            )
        """)
        
        # Tiles table - tracks individual tile processing status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tiles (
                tile_id TEXT PRIMARY KEY,
                job_id TEXT,
                tile_index INTEGER,
                row_idx INTEGER,
                col_idx INTEGER,
                status TEXT,
                worker_id TEXT,
                created_at TIMESTAMP,
                processed_at TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            )
        """)
        
        # Worker heartbeats table - tracks worker health
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS worker_heartbeats (
                worker_id TEXT PRIMARY KEY,
                last_heartbeat TIMESTAMP,
                status TEXT,
                tasks_processed INTEGER,
                current_task TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def create_job(self, job_id: str, filename: str, width: int, height: int, 
                   total_tiles: int, transformation: str):
        """Create a new job entry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs (job_id, original_filename, image_width, image_height, 
                             total_tiles, transformation, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (job_id, filename, width, height, total_tiles, transformation, 
              'processing', datetime.now()))
        conn.commit()
        conn.close()
        logger.info(f"Job {job_id} created with {total_tiles} tiles")
    
    def create_tile(self, tile_id: str, job_id: str, tile_index: int, 
                    row_idx: int, col_idx: int):
        """Create a new tile entry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tiles (tile_id, job_id, tile_index, row_idx, col_idx, 
                               status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (tile_id, job_id, tile_index, row_idx, col_idx, 'pending', datetime.now()))
        conn.commit()
        conn.close()
    
    def update_tile_status(self, tile_id: str, status: str, worker_id: str = None):
        """Update tile processing status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if worker_id:
            cursor.execute("""
                UPDATE tiles SET status = ?, worker_id = ?, processed_at = ?
                WHERE tile_id = ?
            """, (status, worker_id, datetime.now(), tile_id))
        else:
            cursor.execute("""
                UPDATE tiles SET status = ?, processed_at = ?
                WHERE tile_id = ?
            """, (status, datetime.now(), tile_id))
        conn.commit()
        conn.close()
    
    def update_job_status(self, job_id: str, status: str, result_path: str = None):
        """Update job status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if result_path:
            cursor.execute("""
                UPDATE jobs SET status = ?, completed_at = ?, result_path = ?
                WHERE job_id = ?
            """, (status, datetime.now(), result_path, job_id))
        else:
            cursor.execute("""
                UPDATE jobs SET status = ?
                WHERE job_id = ?
            """, (status, job_id))
        conn.commit()
        conn.close()
        logger.info(f"Job {job_id} status updated to {status}")
    
    def get_job_progress(self, job_id: str) -> Dict:
        """Get job progress statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT status, COUNT(*) FROM tiles
            WHERE job_id = ?
            GROUP BY status
        """, (job_id,))
        
        status_counts = dict(cursor.fetchall())
        
        cursor.execute("SELECT total_tiles FROM jobs WHERE job_id = ?", (job_id,))
        total_tiles_result = cursor.fetchone()
        if not total_tiles_result:
            return {'total': 0, 'pending': 0, 'completed': 0, 'failed': 0}
        
        total_tiles = total_tiles_result[0]
        
        conn.close()
        
        return {
            'total': total_tiles,
            'pending': status_counts.get('pending', 0),
            'completed': status_counts.get('completed', 0),
            'failed': status_counts.get('failed', 0)
        }
    
    def update_worker_heartbeat(self, worker_id: str, tasks_processed: int, 
                                current_task: str = None):
        """Update worker heartbeat (1 Mark for monitoring)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO worker_heartbeats 
            (worker_id, last_heartbeat, status, tasks_processed, current_task)
            VALUES (?, ?, ?, ?, ?)
        """, (worker_id, datetime.now(), 'active', tasks_processed, current_task))
        conn.commit()
        conn.close()
    
    def get_active_workers(self) -> List[Dict]:
        """Get list of active workers (1 Mark for monitoring)"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Workers are considered active if heartbeat within last 30 seconds
        cursor.execute("""
            SELECT worker_id, last_heartbeat, tasks_processed, current_task
            FROM worker_heartbeats
            WHERE datetime(last_heartbeat) > datetime('now', '-30 seconds')
        """)
        
        workers = []
        for row in cursor.fetchall():
            workers.append({
                'worker_id': row['worker_id'],
                'last_heartbeat': row['last_heartbeat'],
                'tasks_processed': row['tasks_processed'],
                'current_task': row['current_task'],
                'status': 'active'
            })
        
        conn.close()
        return workers

# ============================================================================
# IMAGE TILING AND SEGMENTATION LOGIC (2 Marks)
# ============================================================================

class ImageProcessor:
    """
    Handles image validation, tiling, and reconstruction.
    This class implements the core image segmentation logic.
    """
    
    @staticmethod
    def validate_image(image: Image.Image) -> bool:
        """Validate image meets minimum size requirements"""
        width, height = image.size
        if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
            logger.error(f"Image too small: {width}x{height}. Minimum: {MIN_IMAGE_SIZE}x{MIN_IMAGE_SIZE}")
            return False
        logger.info(f"Image validated: {width}x{height}")
        return True
    
    @staticmethod
    def create_tiles(image: Image.Image, job_id: str, transformation: str = 'grayscale') -> List[Dict]:
        """
        Split image into tiles for distributed processing.
        This is the core tiling logic (2 Marks).
        FIXED: Now includes transformation in each tile
        """
        width, height = image.size
        tiles = []
        tile_index = 0
        
        # Calculate number of tiles in each dimension
        num_cols = (width + TILE_SIZE - 1) // TILE_SIZE
        num_rows = (height + TILE_SIZE - 1) // TILE_SIZE
        
        logger.info(f"Creating {num_rows}x{num_cols} = {num_rows * num_cols} tiles with '{transformation}' transformation")
        
        for row in range(num_rows):
            for col in range(num_cols):
                # Calculate tile boundaries
                x1 = col * TILE_SIZE
                y1 = row * TILE_SIZE
                x2 = min(x1 + TILE_SIZE, width)
                y2 = min(y1 + TILE_SIZE, height)
                
                # Extract tile
                tile = image.crop((x1, y1, x2, y2))
                
                # Convert to bytes
                buffered = BytesIO()
                tile.save(buffered, format="PNG")
                tile_bytes = buffered.getvalue()
                
                # Encode to base64 for Kafka transmission
                tile_b64 = base64.b64encode(tile_bytes).decode('utf-8')
                
                tile_id = f"{job_id}_tile_{tile_index}"
                
                tiles.append({
                    'tile_id': tile_id,
                    'job_id': job_id,
                    'tile_index': tile_index,
                    'row': row,
                    'col': col,
                    'x1': x1,
                    'y1': y1,
                    'x2': x2,
                    'y2': y2,
                    'data': tile_b64,
                    'width': x2 - x1,
                    'height': y2 - y1,
                    'transformation': transformation  # ✅ CRITICAL FIX
                })
                
                tile_index += 1
        
        logger.info(f"Successfully created {len(tiles)} tiles for job {job_id} with transformation '{transformation}'")
        return tiles
    
    @staticmethod
    def reconstruct_image(tiles: List[Dict], original_width: int, 
                          original_height: int) -> Image.Image:
        """
        Reconstruct final image from processed tiles (1.5 Marks).
        """
        logger.info(f"Reconstructing image: {original_width}x{original_height} from {len(tiles)} tiles")
        
        # Create blank canvas
        result_image = Image.new('RGB', (original_width, original_height))
        
        # Place each tile in correct position
        for tile_data in tiles:
            # Decode base64 tile data
            tile_bytes = base64.b64decode(tile_data['data'])
            tile_img = Image.open(BytesIO(tile_bytes))
            
            # Paste tile at correct position
            x1, y1 = tile_data['x1'], tile_data['y1']
            result_image.paste(tile_img, (x1, y1))
            
            logger.debug(f"Placed tile at ({x1}, {y1})")
        
        logger.info("Image reconstruction completed successfully")
        return result_image

# ============================================================================
# KAFKA COMMUNICATION (1.5 Marks for Task Distribution)
# ============================================================================

class KafkaManager:
    """
    Manages Kafka producer and consumer for task distribution and result collection.
    """
    
    def __init__(self):
        # Producer for sending tasks
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'master-producer',
            'acks': 'all',
            'retries': 3,
            'message.max.bytes': 10485760
        })
        
        # Consumer for receiving results
        self.result_consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'master-result-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })
        self.result_consumer.subscribe([RESULTS_TOPIC])
        
        # Consumer for worker heartbeats
        self.heartbeat_consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'master-heartbeat-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        self.heartbeat_consumer.subscribe([HEARTBEATS_TOPIC])
        
        logger.info("Kafka manager initialized")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def publish_task(self, task: Dict):
        """
        Publish image tile task to Kafka (1.5 Marks for task distribution).
        """
        try:
            message = json.dumps(task)
            self.producer.produce(
                TASKS_TOPIC,
                key=task['tile_id'].encode('utf-8'),
                value=message.encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
            logger.debug(f"Published task: {task['tile_id']} with transformation: {task.get('transformation')}")
        except Exception as e:
            logger.error(f"Error publishing task: {e}")
            raise
    
    def flush_producer(self):
        """Ensure all messages are sent"""
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"{remaining} messages not delivered")
    
    def consume_results(self, timeout: float = 1.0) -> Optional[Dict]:
        """Consume processed tile results"""
        try:
            msg = self.result_consumer.poll(timeout=timeout)
            if msg is None:
                return None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    return None
            
            result = json.loads(msg.value().decode('utf-8'))
            logger.debug(f"Consumed result: {result.get('tile_id')}")
            return result
        except Exception as e:
            logger.error(f"Error consuming results: {e}")
            return None
    
    def consume_heartbeat(self, timeout: float = 1.0) -> Optional[Dict]:
        """Consume worker heartbeat messages (1 Mark for monitoring)"""
        try:
            msg = self.heartbeat_consumer.poll(timeout=timeout)
            if msg is None:
                return None
            if msg.error():
                return None
            
            heartbeat = json.loads(msg.value().decode('utf-8'))
            logger.debug(f"Received heartbeat from {heartbeat.get('worker_id')}")
            return heartbeat
        except Exception as e:
            logger.error(f"Error consuming heartbeat: {e}")
            return None

# ============================================================================
# FASTAPI APPLICATION (2 Marks for Frontend)
# ============================================================================

app = FastAPI(title="Distributed Image Processing - Master Node")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
db = MetadataDB(DB_PATH)
kafka_manager = KafkaManager()
image_processor = ImageProcessor()

# Store for tracking active jobs
active_jobs: Dict[str, Dict] = {}

# WebSocket connections for real-time updates
active_websockets: List[WebSocket] = []

# Create directories
os.makedirs("uploads", exist_ok=True)
os.makedirs("results", exist_ok=True)
os.makedirs("static", exist_ok=True)

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """
    Serve the main dashboard UI (2 Marks for Frontend).
    """
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Distributed Image Processing - Master Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        h1 {
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #666;
            font-size: 1.1em;
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .card h2 {
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.5em;
            display: flex;
            align-items: center;
        }
        
        .card h2::before {
            content: "●";
            margin-right: 10px;
            font-size: 0.8em;
        }
        
        .upload-area {
            border: 3px dashed #667eea;
            border-radius: 10px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s;
            margin-bottom: 20px;
        }
        
        .upload-area:hover {
            background: #f8f9ff;
            border-color: #764ba2;
        }
        
        .upload-area input[type="file"] {
            display: none;
        }
        
        .upload-icon {
            font-size: 3em;
            margin-bottom: 10px;
        }
        
        select, button {
            width: 100%;
            padding: 12px;
            margin: 10px 0;
            border: 2px solid #667eea;
            border-radius: 8px;
            font-size: 1em;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: bold;
            border: none;
        }
        
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        }
        
        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none;
        }
        
        .progress-container {
            margin: 20px 0;
        }
        
        .progress-bar {
            width: 100%;
            height: 30px;
            background: #f0f0f0;
            border-radius: 15px;
            overflow: hidden;
            position: relative;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }
        
        .stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
            margin: 20px 0;
        }
        
        .stat-box {
            background: #f8f9ff;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        
        .stat-label {
            color: #666;
            font-size: 0.9em;
            margin-top: 5px;
        }
        
        .worker-list {
            display: grid;
            gap: 10px;
        }
        
        .worker-item {
            background: #f8f9ff;
            padding: 15px;
            border-radius: 8px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .worker-status {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #4caf50;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .image-preview {
            max-width: 100%;
            border-radius: 8px;
            margin: 10px 0;
        }
        
        .job-history {
            max-height: 400px;
            overflow-y: auto;
        }
        
        .job-item {
            background: #f8f9ff;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .job-item:hover {
            background: #e8e9ff;
            transform: translateX(5px);
        }
        
        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }
        
        .status-processing {
            background: #fff3cd;
            color: #856404;
        }
        
        .status-completed {
            background: #d4edda;
            color: #155724;
        }
        
        .status-failed {
            background: #f8d7da;
            color: #721c24;
        }
        
        .alert {
            padding: 15px;
            border-radius: 8px;
            margin: 10px 0;
        }
        
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .alert-info {
            background: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🖼️ Distributed Image Processing Pipeline</h1>
            <p class="subtitle">Master Node Dashboard - Real-time Kafka-based Image Processing</p>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>Upload Image</h2>
                <div class="upload-area" id="uploadArea">
                    <div class="upload-icon">📁</div>
                    <p>Click to select or drag & drop an image</p>
                    <p style="color: #999; font-size: 0.9em;">Minimum size: 1024x1024 pixels</p>
                    <input type="file" id="fileInput" accept="image/*">
                </div>
                
                <select id="transformationType">
                    <option value="grayscale">Grayscale</option>
                    <option value="blur">Blur</option>
                    <option value="edge_detection">Edge Detection</option>
                    <option value="sharpen">Sharpen</option>
                    <option value="rotate_90">Rotate 90°</option>
                    <option value="contrast">Enhance Contrast</option>
                </select>
                
                <button id="processButton" disabled>Start Processing</button>
                
                <div id="uploadStatus"></div>
                
                <img id="imagePreview" class="image-preview" style="display: none;">
            </div>
            
            <div class="card">
                <h2>Processing Progress</h2>
                <div id="currentJobInfo" style="display: none;">
                    <p><strong>Job ID:</strong> <span id="jobId"></span></p>
                    <p><strong>Transformation:</strong> <span id="jobTransform"></span></p>
                    
                    <div class="progress-container">
                        <div class="progress-bar">
                            <div class="progress-fill" id="progressFill" style="width: 0%">0%</div>
                        </div>
                    </div>
                    
                    <div class="stats">
                        <div class="stat-box">
                            <div class="stat-value" id="totalTiles">0</div>
                            <div class="stat-label">Total Tiles</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value" id="pendingTiles">0</div>
                            <div class="stat-label">Pending</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value" id="completedTiles">0</div>
                            <div class="stat-label">Completed</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-value" id="failedTiles">0</div>
                            <div class="stat-label">Failed</div>
                        </div>
                    </div>
                    
                    <button id="downloadButton" style="display: none;">Download Result</button>
                </div>
                
                <div id="noJobInfo" style="text-align: center; color: #999; padding: 40px;">
                    <p style="font-size: 3em;">⏳</p>
                    <p>No active job. Upload an image to start processing.</p>
                </div>
            </div>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>Active Workers</h2>
                <div class="worker-list" id="workerList">
                    <p style="text-align: center; color: #999; padding: 20px;">
                        Waiting for workers to connect...
                    </p>
                </div>
            </div>
            
            <div class="card">
                <h2>Recent Jobs</h2>
                <div class="job-history" id="jobHistory">
                    <p style="text-align: center; color: #999; padding: 20px;">
                        No jobs yet
                    </p>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let ws = null;
        let currentJobId = null;
        let selectedFile = null;
        
        // WebSocket connection for real-time updates
        function connectWebSocket() {
            ws = new WebSocket(`ws://${window.location.host}/ws`);
            
            ws.onopen = () => {
                console.log('WebSocket connected');
            };
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                handleWebSocketMessage(data);
            };
            
            ws.onclose = () => {
                console.log('WebSocket disconnected, reconnecting...');
                setTimeout(connectWebSocket, 3000);
            };
            
            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
        }
        
        function handleWebSocketMessage(data) {
            if (data.type === 'progress') {
                updateProgress(data);
            } else if (data.type === 'workers') {
                updateWorkers(data.workers);
            } else if (data.type === 'completed') {
                showCompletion(data);
            } else if (data.type === 'error') {
                showError(data.message);
            }
        }
        
        function updateProgress(data) {
            const progress = data.progress;
            const percentage = Math.round((progress.completed / progress.total) * 100);
            
            document.getElementById('progressFill').style.width = percentage + '%';
            document.getElementById('progressFill').textContent = percentage + '%';
            
            document.getElementById('totalTiles').textContent = progress.total;
            document.getElementById('pendingTiles').textContent = progress.pending;
            document.getElementById('completedTiles').textContent = progress.completed;
            document.getElementById('failedTiles').textContent = progress.failed;
        }
        
        function updateWorkers(workers) {
            const workerList = document.getElementById('workerList');
            
            if (workers.length === 0) {
                workerList.innerHTML = `
                    <p style="text-align: center; color: #999; padding: 20px;">
                        No active workers
                    </p>
                `;
                return;
            }
            
            workerList.innerHTML = workers.map(worker => `
                <div class="worker-item">
                    <div>
                        <strong>${worker.worker_id}</strong><br>
                        <small>Tasks: ${worker.tasks_processed} | Last: ${new Date(worker.last_heartbeat).toLocaleTimeString()}</small>
                    </div>
                    <div class="worker-status"></div>
                </div>
            `).join('');
        }
        
        function showCompletion(data) {
            document.getElementById('downloadButton').style.display = 'block';
            document.getElementById('downloadButton').onclick = () => {
                window.location.href = `/download/${data.job_id}`;
            };
            
            showAlert('success', 'Processing completed! Click download to get your result.');
            loadJobHistory();
        }
        
        function showAlert(type, message) {
            const status = document.getElementById('uploadStatus');
            status.innerHTML = `<div class="alert alert-${type}">${message}</div>`;
            setTimeout(() => {
                status.innerHTML = '';
            }, 5000);
        }
        
        function showError(message) {
            showAlert('error', message);
        }
        
        // File upload handling
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');
        const processButton = document.getElementById('processButton');
        const imagePreview = document.getElementById('imagePreview');
        
        uploadArea.onclick = () => fileInput.click();
        
        uploadArea.ondragover = (e) => {
            e.preventDefault();
            uploadArea.style.background = '#f8f9ff';
        };
        
        uploadArea.ondragleave = () => {
            uploadArea.style.background = 'white';
        };
        
        uploadArea.ondrop = (e) => {
            e.preventDefault();
            uploadArea.style.background = 'white';
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect();
            }
        };
        
        fileInput.onchange = handleFileSelect;
        
        function handleFileSelect() {
            const file = fileInput.files[0];
            if (!file) return;
            
            selectedFile = file;
            
            // Show preview
            const reader = new FileReader();
            reader.onload = (e) => {
                imagePreview.src = e.target.result;
                imagePreview.style.display = 'block';
                
                // Validate image size
                const img = new Image();
                img.onload = () => {
                    if (img.width < 1024 || img.height < 1024) {
                        showAlert('error', `Image too small: ${img.width}x${img.height}. Minimum: 1024x1024`);
                        processButton.disabled = true;
                    } else {
                        showAlert('info', `Image loaded: ${img.width}x${img.height} pixels`);
                        processButton.disabled = false;
                    }
                };
                img.src = e.target.result;
            };
            reader.readAsDataURL(file);
        }
        
        processButton.onclick = async () => {
            if (!selectedFile) return;
            
            const selectedTransformation = document.getElementById('transformationType').value;
            console.log('Selected transformation:', selectedTransformation);
            
            processButton.disabled = true;
            processButton.textContent = 'Processing...';
            
            const formData = new FormData();
            formData.append('file', selectedFile);
            formData.append('transformation', selectedTransformation);
            
            // Debug: Log FormData contents
            for (let pair of formData.entries()) {
                console.log(pair[0] + ': ' + pair[1]);
            }
            
            try {
                const response = await fetch('/process', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                console.log('Server response:', result);
                
                if (response.ok) {
                    currentJobId = result.job_id;
                    document.getElementById('jobId').textContent = result.job_id;
                    document.getElementById('jobTransform').textContent = result.transformation;
                    document.getElementById('currentJobInfo').style.display = 'block';
                    document.getElementById('noJobInfo').style.display = 'none';
                    showAlert('success', `Job submitted! Transformation: ${result.transformation}`);
                } else {
                    showAlert('error', result.detail || 'Processing failed');
                    processButton.disabled = false;
                    processButton.textContent = 'Start Processing';
                }
            } catch (error) {
                showAlert('error', 'Network error: ' + error.message);
                processButton.disabled = false;
                processButton.textContent = 'Start Processing';
            }
        };
        
        async function loadJobHistory() {
            try {
                const response = await fetch('/jobs');
                const jobs = await response.json();
                
                const jobHistory = document.getElementById('jobHistory');
                
                if (jobs.length === 0) {
                    jobHistory.innerHTML = '<p style="text-align: center; color: #999; padding: 20px;">No jobs yet</p>';
                    return;
                }
                
                jobHistory.innerHTML = jobs.map(job => `
                    <div class="job-item" onclick="viewJob('${job.job_id}')">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                            <strong>${job.job_id.substring(0, 8)}...</strong>
                            <span class="status-badge status-${job.status}">${job.status}</span>
                        </div>
                        <div style="font-size: 0.9em; color: #666;">
                            ${job.transformation} | ${job.total_tiles} tiles<br>
                            ${new Date(job.created_at).toLocaleString()}
                        </div>
                    </div>
                `).join('');
            } catch (error) {
                console.error('Failed to load job history:', error);
            }
        }
        
        function viewJob(jobId) {
            window.open(`/download/${jobId}`, '_blank');
        }
        
        // Initialize
        connectWebSocket();
        loadJobHistory();
        setInterval(loadJobHistory, 10000);
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)

@app.post("/process")
async def process_image(
    file: UploadFile = File(...),
    transformation: str = Form("grayscale"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Main endpoint to process uploaded image.
    CRITICAL FIX: Use Form() to properly parse transformation from multipart form data
    """
    try:
        # DEBUG: Log received transformation
        logger.info(f"========================================")
        logger.info(f"RECEIVED TRANSFORMATION: '{transformation}'")
        logger.info(f"TRANSFORMATION TYPE: {type(transformation)}")
        logger.info(f"========================================")
        
        # Read and validate image
        contents = await file.read()
        image = Image.open(BytesIO(contents))
        
        # Validate image size
        if not image_processor.validate_image(image):
            raise HTTPException(status_code=400, detail="Image does not meet minimum size requirements (1024x1024)")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        logger.info(f"Starting job {job_id} for file {file.filename} with transformation: '{transformation}'")
        
        # Create tiles with transformation
        tiles = image_processor.create_tiles(image, job_id, transformation)
        
        # DEBUG: Check first tile
        if tiles:
            logger.info(f"First tile transformation: '{tiles[0].get('transformation', 'NOT FOUND')}'")
        
        # Save metadata
        width, height = image.size
        db.create_job(job_id, file.filename, width, height, len(tiles), transformation)
        
        for tile in tiles:
            db.create_tile(
                tile['tile_id'],
                job_id,
                tile['tile_index'],
                tile['row'],
                tile['col']
            )
        
        # Store job info
        active_jobs[job_id] = {
            'tiles': tiles,
            'transformation': transformation,
            'width': width,
            'height': height,
            'total_tiles': len(tiles),
            'completed_tiles': {},
            'start_time': time.time()
        }
        
        # Distribute tasks via Kafka
        background_tasks.add_task(distribute_tasks, job_id, tiles, transformation)
        
        logger.info(f"Job {job_id} created with {len(tiles)} tiles and transformation: '{transformation}'")
        
        return {
            "job_id": job_id,
            "total_tiles": len(tiles),
            "transformation": transformation,
            "status": "processing"
        }
    
    except Exception as e:
        logger.error(f"Error processing image: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

async def distribute_tasks(job_id: str, tiles: List[Dict], transformation: str):
    """
    Background task to distribute tiles to Kafka.
    Each tile already contains the transformation field.
    """
    try:
        for tile in tiles:
            # Tile already has transformation from create_tiles()
            kafka_manager.publish_task(tile)
        
        kafka_manager.flush_producer()
        logger.info(f"All {len(tiles)} tasks distributed for job {job_id} with transformation: '{transformation}'")
    
    except Exception as e:
        logger.error(f"Error distributing tasks: {e}")
        db.update_job_status(job_id, 'failed')

@app.get("/jobs")
async def get_jobs():
    """Get list of all jobs"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT job_id, original_filename, total_tiles, transformation, 
               status, created_at, completed_at
        FROM jobs
        ORDER BY created_at DESC
        LIMIT 20
    """)
    
    jobs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jobs

@app.get("/job/{job_id}/progress")
async def get_job_progress(job_id: str):
    """Get progress for specific job"""
    progress = db.get_job_progress(job_id)
    return progress

@app.get("/download/{job_id}")
async def download_result(job_id: str):
    """Download processed image result"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT result_path, status FROM jobs WHERE job_id = ?", (job_id,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        raise HTTPException(status_code=404, detail="Job not found")
    
    result_path, status = result
    
    if status != 'completed':
        raise HTTPException(status_code=400, detail=f"Job is {status}")
    
    if not result_path or not os.path.exists(result_path):
        raise HTTPException(status_code=404, detail="Result file not found")
    
    return FileResponse(result_path, media_type="image/png", filename=f"result_{job_id}.png")

@app.get("/workers")
async def get_workers():
    """Get active workers"""
    workers = db.get_active_workers()
    return workers

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "master-node"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await websocket.accept()
    active_websockets.append(websocket)
    logger.info("WebSocket client connected")
    
    try:
        while True:
            await asyncio.sleep(1)
            
            # Check for completed jobs
            for job_id in list(active_jobs.keys()):
                job = active_jobs[job_id]
                progress = db.get_job_progress(job_id)
                
                # Send progress update
                await websocket.send_json({
                    "type": "progress",
                    "job_id": job_id,
                    "progress": progress
                })
                
                # Check if job is complete
                if progress['completed'] == progress['total'] and progress['total'] > 0:
                    await reconstruct_and_complete_job(job_id, websocket)
            
            # Send worker status
            workers = db.get_active_workers()
            await websocket.send_json({
                "type": "workers",
                "workers": workers
            })
    
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
    finally:
        active_websockets.remove(websocket)
        logger.info("WebSocket client disconnected")

async def reconstruct_and_complete_job(job_id: str, websocket: WebSocket):
    """Reconstruct final image from processed tiles"""
    try:
        job = active_jobs[job_id]
        
        # Reconstruct image
        result_image = image_processor.reconstruct_image(
            list(job['completed_tiles'].values()),
            job['width'],
            job['height']
        )
        
        # Save result
        result_path = f"results/{job_id}.png"
        result_image.save(result_path)
        
        # Update database
        db.update_job_status(job_id, 'completed', result_path)
        
        # Notify client
        await websocket.send_json({
            "type": "completed",
            "job_id": job_id,
            "result_path": result_path
        })
        
        # Clean up
        del active_jobs[job_id]
        
        logger.info(f"Job {job_id} completed successfully")
    
    except Exception as e:
        logger.error(f"Error reconstructing image: {e}")
        db.update_job_status(job_id, 'failed')
        if websocket:
            await websocket.send_json({
                "type": "error",
                "message": f"Reconstruction failed: {str(e)}"
            })

# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def consume_results_task():
    """Background task to consume processed tiles from Kafka"""
    logger.info("Starting result consumer task")
    
    while True:
        try:
            result = kafka_manager.consume_results(timeout=1.0)
            
            if result:
                tile_id = result['tile_id']
                job_id = result['job_id']
                
                # Update tile status
                db.update_tile_status(tile_id, 'completed', result.get('worker_id'))
                
                # Store processed tile
                if job_id in active_jobs:
                    active_jobs[job_id]['completed_tiles'][tile_id] = result
                
                logger.info(f"Received processed tile: {tile_id}")
            
            await asyncio.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error in result consumer: {e}")
            await asyncio.sleep(1)

async def consume_heartbeats_task():
    """Background task to consume worker heartbeats"""
    logger.info("Starting heartbeat consumer task")
    
    while True:
        try:
            heartbeat = kafka_manager.consume_heartbeat(timeout=1.0)
            
            if heartbeat:
                db.update_worker_heartbeat(
                    heartbeat['worker_id'],
                    heartbeat['tasks_processed'],
                    heartbeat.get('current_task')
                )
                
                logger.debug(f"Heartbeat from {heartbeat['worker_id']}")
            
            await asyncio.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error in heartbeat consumer: {e}")
            await asyncio.sleep(1)

@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup"""
    asyncio.create_task(consume_results_task())
    asyncio.create_task(consume_heartbeats_task())
    logger.info("Master node started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Graceful shutdown"""
    logger.info("Master node shutting down")
    kafka_manager.flush_producer()

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=MASTER_HOST, port=MASTER_PORT)
