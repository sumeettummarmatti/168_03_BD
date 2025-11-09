#!/usr/bin/env python3

"""
NODE 3 & NODE 4 - WORKER NODES
Distributed Image Processing Pipeline with Kafka

FIXED VERSION - Better error handling and stability

Author: Person 3 & Person 4 (Same code, different WORKER_ID)
"""

import os
import sys
import json
import base64
import time
import logging
import signal
from datetime import datetime
from io import BytesIO
from typing import Optional
import numpy as np
from PIL import Image, ImageFilter, ImageEnhance
import cv2

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import threading

# ============================================================================
# CONFIGURATION SECTION
# ============================================================================

# Kafka Configuration - UPDATE THIS WITH YOUR KAFKA BROKER IP
KAFKA_BROKER = "10.148.167.36:9092"  # Update with actual Kafka broker ZeroTier IP
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"

# Worker Configuration
WORKER_ID = "worker_2"  # Change to "worker_2" for second worker
WORKER_PORT = 8003  # Change to 8003 for second worker

# Heartbeat Configuration
HEARTBEAT_INTERVAL = 5  # seconds

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{WORKER_ID}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_flag = False

# Statistics
tasks_processed = 0
current_task_id = None
start_time = time.time()
task_history = []
MAX_HISTORY = 50

# Image storage for UI display
current_images = {
    'original': None,
    'processed': None,
    'task_info': None
}
image_lock = threading.Lock()

# ============================================================================
# IMAGE TRANSFORMATION IMPLEMENTATIONS (5 Marks)
# ============================================================================

class ImageTransformer:
    """
    Implements various image transformations.
    This class demonstrates image processing skills (5 Marks).
    """
    
    @staticmethod
    def grayscale(image: Image.Image) -> Image.Image:
        """Convert image to grayscale"""
        logger.debug("Applying grayscale transformation")
        return image.convert('L').convert('RGB')
    
    @staticmethod
    def blur(image: Image.Image) -> Image.Image:
        """Apply Gaussian blur to image"""
        logger.debug("Applying blur transformation")
        return image.filter(ImageFilter.GaussianBlur(radius=5))
    
    @staticmethod
    def edge_detection(image: Image.Image) -> Image.Image:
        """Detect edges using Sobel operator via OpenCV"""
        logger.debug("Applying edge detection transformation")
        
        # Convert PIL to numpy array
        img_array = np.array(image)
        
        # Convert to grayscale for edge detection
        gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
        
        # Apply Sobel edge detection
        sobelx = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
        sobely = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)
        
        # Combine gradients
        sobel = np.sqrt(sobelx**2 + sobely**2)
        sobel = np.uint8(sobel)
        
        # Convert back to PIL Image
        result = Image.fromarray(sobel).convert('RGB')
        return result
    
    @staticmethod
    def sharpen(image: Image.Image) -> Image.Image:
        """Sharpen image using convolution filter"""
        logger.debug("Applying sharpen transformation")
        
        # Sharpen filter kernel
        sharpen_filter = ImageFilter.Kernel(
            size=(3, 3),
            kernel=[
                -1, -1, -1,
                -1,  9, -1,
                -1, -1, -1
            ],
            scale=1
        )
        
        return image.filter(sharpen_filter)
    
    @staticmethod
    def rotate_90(image: Image.Image) -> Image.Image:
        """Rotate image 90 degrees clockwise"""
        logger.debug("Applying rotate transformation")
        return image.rotate(-90, expand=True)
    
    @staticmethod
    def enhance_contrast(image: Image.Image) -> Image.Image:
        """Enhance image contrast"""
        logger.debug("Applying contrast enhancement")
        enhancer = ImageEnhance.Contrast(image)
        return enhancer.enhance(1.5)
    
    @staticmethod
    def apply_transformation(image: Image.Image, transformation: str) -> Image.Image:
        """Apply specified transformation to image"""
        transformations = {
            'grayscale': ImageTransformer.grayscale,
            'blur': ImageTransformer.blur,
            'edge_detection': ImageTransformer.edge_detection,
            'sharpen': ImageTransformer.sharpen,
            'rotate_90': ImageTransformer.rotate_90,
            'contrast': ImageTransformer.enhance_contrast
        }
        
        if transformation not in transformations:
            logger.warning(f"Unknown transformation: {transformation}, using grayscale")
            transformation = 'grayscale'
        
        try:
            result = transformations[transformation](image)
            logger.info(f"Successfully applied {transformation} transformation")
            return result
        except Exception as e:
            logger.error(f"Error applying {transformation}: {e}")
            # Return original image on error
            return image

# ============================================================================
# KAFKA COMMUNICATION (3 Marks)
# ============================================================================

class KafkaWorker:
    """
    Handles Kafka consumer and producer for task processing.
    FIXED: Better error handling and connection management
    """
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.init_kafka()
        
    def init_kafka(self):
        """Initialize Kafka connections with better error handling"""
        try:
            # Consumer configuration
            self.consumer = Consumer({
                'bootstrap.servers': KAFKA_BROKER,
                'group.id': 'image-workers',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'max.poll.interval.ms': 300000,
                'session.timeout.ms': 30000,
                'client.id': self.worker_id
            })
            
            # Subscribe to tasks topic
            self.consumer.subscribe([TASKS_TOPIC])
            logger.info(f"Consumer subscribed to {TASKS_TOPIC} topic")
            
            # Producer configuration
            self.producer = Producer({
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': f'{self.worker_id}-producer',
                'acks': 'all',
                'retries': 5,
                'retry.backoff.ms': 1000,
                'max.in.flight.requests.per.connection': 1,
                'socket.keepalive.enable': True
            })
            
            logger.info(f"Kafka worker initialized: {self.worker_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka: {e}")
            raise
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'❌ Message delivery FAILED: {err}')
            logger.error(f'   Topic: {msg.topic()}, Key: {msg.key()}')
        else:
            logger.info(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}')
    
    def consume_task(self, timeout: float = 1.0) -> Optional[dict]:
        """Consume task from Kafka with error handling"""
        try:
            msg = self.consumer.poll(timeout=timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                    return None
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    return None
            
            # Parse task message
            task = json.loads(msg.value().decode('utf-8'))
            logger.info(f"✅ Consumed task: {task['tile_id']} for job {task['job_id']}")
            logger.info(f"   Transformation: {task.get('transformation', 'NOT FOUND')}")
            
            return task
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None
        except Exception as e:
            logger.error(f"Error consuming task: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def publish_result(self, result: dict):
        """Publish processed result to Kafka with extensive error handling"""
        try:
            logger.info(f"📤 Publishing result for tile: {result['tile_id']}")
            
            message = json.dumps(result)
            
            # Publish with callback
            self.producer.produce(
                RESULTS_TOPIC,
                key=result['tile_id'].encode('utf-8'),
                value=message.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # CRITICAL: Poll to trigger callbacks and handle errors
            self.producer.poll(0)
            
            # Flush to ensure message is sent
            remaining = self.producer.flush(timeout=10)
            
            if remaining > 0:
                logger.error(f"❌ Failed to deliver {remaining} messages!")
            else:
                logger.info(f"✅ Result published and flushed successfully: {result['tile_id']}")
                
        except BufferError as e:
            logger.error(f"Buffer full error - message queue full: {e}")
            time.sleep(1)
            self.producer.flush(timeout=10)
            
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR publishing result: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def publish_heartbeat(self):
        """Publish heartbeat message"""
        try:
            heartbeat = {
                'worker_id': self.worker_id,
                'timestamp': datetime.now().isoformat(),
                'status': 'active',
                'tasks_processed': tasks_processed,
                'current_task': current_task_id
            }
            
            message = json.dumps(heartbeat)
            self.producer.produce(
                HEARTBEATS_TOPIC,
                key=self.worker_id.encode('utf-8'),
                value=message.encode('utf-8')
            )
            self.producer.poll(0)
            logger.debug(f"💓 Heartbeat sent: {tasks_processed} tasks processed")
            
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")
    
    def flush(self):
        """Ensure all messages are sent"""
        logger.info("Flushing producer...")
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages not delivered")
        else:
            logger.info("✅ All messages flushed successfully")
    
    def close(self):
        """Clean shutdown of Kafka connections"""
        logger.info("Closing Kafka connections...")
        self.flush()
        self.consumer.close()
        logger.info("Kafka connections closed")

# ============================================================================
# WORKER TASK PROCESSING
# ============================================================================

class ImageWorker:
    """Main worker class that coordinates task processing"""
    
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.kafka_worker = KafkaWorker(worker_id)
        self.transformer = ImageTransformer()
        self.running = True
        
        logger.info(f"Image worker initialized: {worker_id}")
    
    def process_tile(self, task: dict) -> dict:
        """Process a single image tile"""
        global current_task_id, tasks_processed, task_history, current_images
        
        tile_id = task['tile_id']
        current_task_id = tile_id
        
        try:
            logger.info(f"🔧 Processing tile: {tile_id}")
            logger.info(f"   Job ID: {task['job_id']}")
            logger.info(f"   Transformation: {task.get('transformation', 'NOT FOUND')}")
            
            start_time_local = time.time()
            
            # Decode base64 image data
            image_data = base64.b64decode(task['data'])
            image = Image.open(BytesIO(image_data))
            
            # Store original image for UI
            with image_lock:
                current_images['original'] = task['data']
                current_images['task_info'] = {
                    'tile_id': tile_id,
                    'job_id': task['job_id'],
                    'transformation': task.get('transformation', 'grayscale'),
                    'tile_index': task.get('tile_index', 0),
                    'dimensions': f"{task.get('width', 0)}x{task.get('height', 0)}"
                }
            
            # Apply transformation
            transformation = task.get('transformation', 'grayscale')
            processed_image = self.transformer.apply_transformation(image, transformation)
            
            # Encode result back to base64
            buffered = BytesIO()
            processed_image.save(buffered, format="PNG")
            processed_data = base64.b64encode(buffered.getvalue()).decode('utf-8')
            
            # Store processed image for UI
            with image_lock:
                current_images['processed'] = processed_data
            
            processing_time = time.time() - start_time_local
            
            # Prepare result
            result = {
                'tile_id': tile_id,
                'job_id': task['job_id'],
                'tile_index': task['tile_index'],
                'row': task['row'],
                'col': task['col'],
                'x1': task['x1'],
                'y1': task['y1'],
                'x2': task['x2'],
                'y2': task['y2'],
                'width': task['width'],
                'height': task['height'],
                'data': processed_data,
                'worker_id': self.worker_id,
                'processing_time': processing_time,
                'transformation': transformation,
                'timestamp': datetime.now().isoformat()
            }
            
            tasks_processed += 1
            
            # Add to history
            task_history.append({
                'tile_id': tile_id,
                'job_id': task['job_id'],
                'transformation': transformation,
                'processing_time': processing_time,
                'timestamp': datetime.now().isoformat()
            })
            if len(task_history) > MAX_HISTORY:
                task_history.pop(0)
            
            current_task_id = None
            
            logger.info(f"✅ Tile {tile_id} processed in {processing_time:.2f}s with {transformation}")
            return result
        
        except Exception as e:
            logger.error(f"❌ ERROR processing tile {tile_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            current_task_id = None
            raise
    
    def run(self):
        """Main worker loop"""
        logger.info(f"🚀 Worker {self.worker_id} started")
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        while self.running and not shutdown_flag:
            try:
                # Consume task
                task = self.kafka_worker.consume_task(timeout=1.0)
                
                if task is None:
                    continue
                
                # Process task
                result = self.process_tile(task)
                
                # Publish result - WITH ERROR HANDLING
                try:
                    self.kafka_worker.publish_result(result)
                except Exception as e:
                    logger.error(f"❌ FAILED to publish result: {e}")
                    continue
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                logger.error(f"❌ Error in worker loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(1)
        
        logger.info("Worker loop ended")
    
    def heartbeat_loop(self):
        """Separate thread for sending heartbeats"""
        logger.info("💓 Heartbeat loop started")
        
        while self.running and not shutdown_flag:
            try:
                self.kafka_worker.publish_heartbeat()
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(HEARTBEAT_INTERVAL)
        
        logger.info("Heartbeat loop ended")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("🛑 Shutting down worker...")
        self.running = False
        
        # Send final heartbeat
        try:
            self.kafka_worker.publish_heartbeat()
        except:
            pass
        
        # Close Kafka connections
        self.kafka_worker.close()
        logger.info("Worker shutdown complete")

# ============================================================================
# SIGNAL HANDLERS
# ============================================================================

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_flag
    logger.info(f"Received signal {signum}")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# WEB UI
# ============================================================================

from http.server import HTTPServer, BaseHTTPRequestHandler
import json as json_lib

class StatusHandler(BaseHTTPRequestHandler):
    """HTTP handler with UI and image viewer"""
    
    def do_GET(self):
        if self.path == '/' or self.path == '/index.html':
            self.serve_dashboard()
        elif self.path == '/api/status':
            self.serve_status_json()
        elif self.path == '/api/history':
            self.serve_history_json()
        elif self.path == '/api/current-images':
            self.serve_current_images()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'404 - Not Found')
    
    def serve_dashboard(self):
        """Serve the main dashboard HTML with image viewer"""
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Worker {WORKER_ID} - Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .header h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header .subtitle {{
            color: #666;
            font-size: 1.1em;
        }}
        
        .status-indicator {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }}
        
        .status-active {{
            background: #10b981;
            box-shadow: 0 0 10px #10b981;
        }}
        
        .status-shutdown {{
            background: #ef4444;
            box-shadow: 0 0 10px #ef4444;
        }}
        
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .card h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.3em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        
        .metric {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid #f0f0f0;
        }}
        
        .metric:last-child {{
            border-bottom: none;
        }}
        
        .metric-label {{
            color: #666;
            font-weight: 500;
        }}
        
        .metric-value {{
            color: #333;
            font-weight: 700;
            font-size: 1.2em;
        }}
        
        .metric-value.highlight {{
            color: #667eea;
        }}
        
        .image-viewer {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 20px;
        }}
        
        .image-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-top: 20px;
        }}
        
        .image-container {{
            border: 2px solid #e5e7eb;
            border-radius: 10px;
            padding: 15px;
            background: #f9fafb;
        }}
        
        .image-container h3 {{
            color: #667eea;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        
        .image-wrapper {{
            position: relative;
            width: 100%;
            min-height: 300px;
            background: #e5e7eb;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            overflow: hidden;
        }}
        
        .image-wrapper img {{
            max-width: 100%;
            max-height: 400px;
            object-fit: contain;
            border-radius: 5px;
        }}
        
        .image-placeholder {{
            color: #9ca3af;
            font-size: 1.1em;
            text-align: center;
            padding: 40px 20px;
        }}
        
        .task-info {{
            background: #f3f4f6;
            padding: 15px;
            border-radius: 8px;
            margin-top: 15px;
        }}
        
        .task-info-item {{
            display: flex;
            justify-content: space-between;
            padding: 5px 0;
            font-size: 0.9em;
        }}
        
        .task-info-label {{
            color: #6b7280;
            font-weight: 500;
        }}
        
        .task-info-value {{
            color: #1f2937;
            font-weight: 600;
        }}
        
        .table-container {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow-x: auto;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        th {{
            background: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }}
        
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #f0f0f0;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        
        .badge-grayscale {{ background: #6b7280; color: white; }}
        .badge-blur {{ background: #3b82f6; color: white; }}
        .badge-edge_detection {{ background: #ef4444; color: white; }}
        .badge-sharpen {{ background: #f59e0b; color: white; }}
        .badge-rotate_90 {{ background: #8b5cf6; color: white; }}
        .badge-contrast {{ background: #10b981; color: white; }}
        
        .no-data {{
            text-align: center;
            padding: 40px;
            color: #999;
            font-style: italic;
        }}
        
        .refresh-info {{
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 0.9em;
        }}
        
        @media (max-width: 1024px) {{
            .image-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        @media (max-width: 768px) {{
            .grid {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 1.8em;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔧 Worker Node Dashboard</h1>
            <p class="subtitle">
                <span class="status-indicator status-active"></span>
                <span id="worker-id">{WORKER_ID}</span> • Real-time monitoring
            </p>
        </div>
        
        <div class="image-viewer">
            <h2>🖼️ Live Image Processing</h2>
            <div id="task-info-display" class="task-info" style="display: none;">
                <div class="task-info-item">
                    <span class="task-info-label">Current Task:</span>
                    <span class="task-info-value" id="current-tile-id">-</span>
                </div>
                <div class="task-info-item">
                    <span class="task-info-label">Job ID:</span>
                    <span class="task-info-value" id="current-job-id">-</span>
                </div>
                <div class="task-info-item">
                    <span class="task-info-label">Transformation:</span>
                    <span class="task-info-value" id="current-transformation">-</span>
                </div>
                <div class="task-info-item">
                    <span class="task-info-label">Tile Dimensions:</span>
                    <span class="task-info-value" id="current-dimensions">-</span>
                </div>
            </div>
            <div class="image-grid">
                <div class="image-container">
                    <h3>📥 Original Tile</h3>
                    <div class="image-wrapper" id="original-image-wrapper">
                        <div class="image-placeholder">
                            Waiting for tasks...<br>
                            <small>Original image will appear here</small>
                        </div>
                    </div>
                </div>
                <div class="image-container">
                    <h3>✨ Processed Tile</h3>
                    <div class="image-wrapper" id="processed-image-wrapper">
                        <div class="image-placeholder">
                            Waiting for tasks...<br>
                            <small>Processed image will appear here</small>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>📊 Performance Metrics</h2>
                <div class="metric">
                    <span class="metric-label">Tasks Processed</span>
                    <span class="metric-value highlight" id="tasks-processed">0</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Uptime</span>
                    <span class="metric-value" id="uptime">0s</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Avg Processing Time</span>
                    <span class="metric-value" id="avg-time">0ms</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Tasks/Minute</span>
                    <span class="metric-value" id="tasks-per-min">0</span>
                </div>
            </div>
            
            <div class="card">
                <h2>🔗 Kafka Configuration</h2>
                <div class="metric">
                    <span class="metric-label">Broker</span>
                    <span class="metric-value" style="font-size: 0.9em;">{KAFKA_BROKER}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Consumer Group</span>
                    <span class="metric-value" style="font-size: 0.9em;">image-workers</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Tasks Topic</span>
                    <span class="metric-value" style="font-size: 0.9em;">{TASKS_TOPIC}</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Results Topic</span>
                    <span class="metric-value" style="font-size: 0.9em;">{RESULTS_TOPIC}</span>
                </div>
            </div>
            
            <div class="card">
                <h2>⚡ Current Status</h2>
                <div class="metric">
                    <span class="metric-label">Worker Status</span>
                    <span class="metric-value" id="worker-status">Active</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Current Task</span>
                    <span class="metric-value" id="current-task" style="font-size: 0.9em;">Idle</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Heartbeat Interval</span>
                    <span class="metric-value">{HEARTBEAT_INTERVAL}s</span>
                </div>
                <div class="metric">
                    <span class="metric-label">Last Update</span>
                    <span class="metric-value" id="last-update" style="font-size: 0.9em;">--</span>
                </div>
            </div>
        </div>
        
        <div class="table-container">
            <h2>📜 Recent Task History</h2>
            <table id="history-table">
                <thead>
                    <tr>
                        <th>Tile ID</th>
                        <th>Job ID</th>
                        <th>Transformation</th>
                        <th>Processing Time</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody id="history-body">
                    <tr><td colspan="5" class="no-data">No tasks processed yet</td></tr>
                </tbody>
            </table>
        </div>
        
        <div class="refresh-info">
            🔄 Auto-refreshing every 1 second
        </div>
    </div>
    
    <script>
        const startTime = Date.now();
        
        function formatDuration(seconds) {{
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            const secs = Math.floor(seconds % 60);
            
            if (hours > 0) return `${{hours}}h ${{minutes}}m ${{secs}}s`;
            if (minutes > 0) return `${{minutes}}m ${{secs}}s`;
            return `${{secs}}s`;
        }}
        
        function getTransformationBadge(type) {{
            const badges = {{
                'grayscale': 'badge-grayscale',
                'blur': 'badge-blur',
                'edge_detection': 'badge-edge_detection',
                'sharpen': 'badge-sharpen',
                'rotate_90': 'badge-rotate_90',
                'contrast': 'badge-contrast'
            }};
            return badges[type] || 'badge-grayscale';
        }}
        
        async function updateImages() {{
            try {{
                const response = await fetch('/api/current-images');
                const data = await response.json();
                
                const taskInfoDisplay = document.getElementById('task-info-display');
                const originalWrapper = document.getElementById('original-image-wrapper');
                const processedWrapper = document.getElementById('processed-image-wrapper');
                
                if (data.has_images) {{
                    // Show task info
                    taskInfoDisplay.style.display = 'block';
                    document.getElementById('current-tile-id').textContent = data.task_info.tile_id;
                    document.getElementById('current-job-id').textContent = data.task_info.job_id;
                    document.getElementById('current-transformation').textContent = data.task_info.transformation;
                    document.getElementById('current-dimensions').textContent = data.task_info.dimensions;
                    
                    // Update original image
                    originalWrapper.innerHTML = `<img src="data:image/png;base64,${{data.original}}" alt="Original">`;
                    
                    // Update processed image
                    processedWrapper.innerHTML = `<img src="data:image/png;base64,${{data.processed}}" alt="Processed">`;
                }} else {{
                    taskInfoDisplay.style.display = 'none';
                    
                    originalWrapper.innerHTML = `
                        <div class="image-placeholder">
                            Waiting for tasks...<br>
                            <small>Original image will appear here</small>
                        </div>
                    `;
                    
                    processedWrapper.innerHTML = `
                        <div class="image-placeholder">
                            Waiting for tasks...<br>
                            <small>Processed image will appear here</small>
                        </div>
                    `;
                }}
            }} catch (error) {{
                console.error('Error updating images:', error);
            }}
        }}
        
        async function updateDashboard() {{
            try {{
                const statusResponse = await fetch('/api/status');
                const status = await statusResponse.json();
                
                document.getElementById('tasks-processed').textContent = status.tasks_processed;
                document.getElementById('uptime').textContent = formatDuration(status.uptime);
                document.getElementById('worker-status').textContent = status.status === 'running' ? 'Active' : 'Shutting Down';
                document.getElementById('current-task').textContent = status.current_task || 'Idle';
                document.getElementById('last-update').textContent = new Date(status.timestamp).toLocaleTimeString();
                document.getElementById('avg-time').textContent = status.avg_processing_time;
                document.getElementById('tasks-per-min').textContent = status.tasks_per_minute;
                
                // Update status indicator
                const indicator = document.querySelector('.status-indicator');
                if (status.status === 'running') {{
                    indicator.classList.remove('status-shutdown');
                    indicator.classList.add('status-active');
                }} else {{
                    indicator.classList.remove('status-active');
                    indicator.classList.add('status-shutdown');
                }}
                
                // Update history
                const historyResponse = await fetch('/api/history');
                const history = await historyResponse.json();
                
                const tbody = document.getElementById('history-body');
                if (history.length === 0) {{
                    tbody.innerHTML = '<tr><td colspan="5" class="no-data">No tasks processed yet</td></tr>';
                }} else {{
                    tbody.innerHTML = history.map(task => `
                        <tr>
                            <td>${{task.tile_id}}</td>
                            <td>${{task.job_id}}</td>
                            <td><span class="badge ${{getTransformationBadge(task.transformation)}}">${{task.transformation}}</span></td>
                            <td>${{task.processing_time}}s</td>
                            <td>${{new Date(task.timestamp).toLocaleString()}}</td>
                        </tr>
                    `).join('');
                }}
            }} catch (error) {{
                console.error('Error updating dashboard:', error);
            }}
        }}
        
        async function updateAll() {{
            await updateDashboard();
            await updateImages();
        }}
        
        // Initial update
        updateAll();
        
        // Auto-refresh every 1 second for real-time feel
        setInterval(updateAll, 1000);
    </script>
</body>
</html>
        """
        
        self.wfile.write(html.encode())
    
    def serve_status_json(self):
        """Serve status data as JSON"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        uptime = time.time() - start_time
        
        # Calculate average processing time
        avg_time = 0
        if task_history:
            avg_time = sum(t['processing_time'] for t in task_history) / len(task_history)
        
        # Calculate tasks per minute
        tasks_per_min = 0
        if uptime > 0:
            tasks_per_min = round((tasks_processed / uptime) * 60, 2)
        
        status = {
            'worker_id': WORKER_ID,
            'status': 'running' if not shutdown_flag else 'shutting_down',
            'tasks_processed': tasks_processed,
            'current_task': current_task_id,
            'timestamp': datetime.now().isoformat(),
            'uptime': round(uptime, 2),
            'avg_processing_time': f"{avg_time:.3f}s" if avg_time > 0 else "0s",
            'tasks_per_minute': tasks_per_min
        }
        
        self.wfile.write(json_lib.dumps(status, indent=2).encode())
    
    def serve_history_json(self):
        """Serve task history as JSON"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        # Create a new list with formatted time (BUG FIX)
        recent_history = [
            {**task, 'processing_time': f"{task['processing_time']:.3f}"} 
            for task in reversed(task_history[-20:])
        ]
        
        self.wfile.write(json_lib.dumps(recent_history, indent=2).encode())
    
    def serve_current_images(self):
        """Serve current processing images"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        with image_lock:
            response = {
                'has_images': current_images['original'] is not None and current_images['processed'] is not None,
                'original': current_images['original'],
                'processed': current_images['processed'],
                'task_info': current_images['task_info']
            }
        
        self.wfile.write(json_lib.dumps(response).encode())
    
    def log_message(self, format, *args):
        """Suppress HTTP logs"""
        pass

def start_status_server():
    """Start HTTP status server with UI"""
    try:
        server = HTTPServer(('0.0.0.0', WORKER_PORT), StatusHandler)
        logger.info(f"Web UI server started on http://0.0.0.0:{WORKER_PORT}")
        logger.info(f"Access dashboard at http://localhost:{WORKER_PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Status server error: {e}")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main function"""
    logger.info("=" * 70)
    logger.info(f"STARTING IMAGE PROCESSING WORKER: {WORKER_ID}")
    logger.info("=" * 70)
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Tasks Topic: {TASKS_TOPIC}")
    logger.info(f"Results Topic: {RESULTS_TOPIC}")
    logger.info(f"Heartbeats Topic: {HEARTBEATS_TOPIC}")
    logger.info(f"Web UI Port: {WORKER_PORT}")
    logger.info(f"Dashboard URL: http://localhost:{WORKER_PORT}")
    logger.info("=" * 70)
    
    # Start status server in background thread
    status_thread = threading.Thread(target=start_status_server, daemon=True)
    status_thread.start()
    
    # Create and run worker
    worker = ImageWorker(WORKER_ID)
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        worker.shutdown()
    
    logger.info(f"Worker {WORKER_ID} stopped")
    logger.info(f"Total tasks processed: {tasks_processed}")

if __name__ == "__main__":
    main()