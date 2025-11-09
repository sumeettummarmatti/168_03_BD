# Distributed Image Processing Pipeline with Apache Kafka

A scalable, fault-tolerant distributed system for parallel image processing using Apache Kafka as the message broker. This project demonstrates real-world distributed systems concepts, including load balancing, asynchronous processing, and worker health monitoring.

📋 Table of Contents
Features
System Architecture
Prerequisites
Installation
Configuration
Usage
Image Transformations
API Documentation
Testing
Troubleshooting
Contributing
License
✨ Features
Distributed Processing: Multiple worker nodes process image tiles in parallel
Kafka-Based Communication: Reliable message queue for task distribution
Load Balancing: Automatic work distribution via Kafka consumer groups
Fault Tolerance: System continues operation even if workers fail
Real-Time Monitoring: Live dashboard showing processing progress and worker health
Multiple Transformations: Grayscale, blur, edge detection, sharpen, rotate
Scalable Architecture: Easy to add more workers for increased throughput
Web Interface: Modern, responsive UI for image upload and result retrieval
🏗️ System Architecture
┌─────────────────────────────────────────────────────────┐
│                    Client (Web Browser)                  │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP/WebSocket
                         ▼
┌─────────────────────────────────────────────────────────┐
│              Master Node (FastAPI Server)                │
│  • Image Upload & Validation                            │
│  • Image Tiling (512x512)                               │
│  • Task Distribution                                     │
│  • Result Reconstruction                                 │
│  • Worker Monitoring                                     │
└───────────┬─────────────────────────────┬───────────────┘
            │                             │
            │ Kafka Producer              │ Kafka Consumer
            ▼                             ▼
┌─────────────────────────────────────────────────────────┐
│                    Kafka Broker                          │
│  Topics:                                                 │
│  • tasks (2 partitions)   - Image tiles to process      │
│  • results (1 partition)  - Processed tiles             │
│  • heartbeats (1 partition) - Worker health status      │
└──────────┬──────────────────────────────┬───────────────┘
           │                              │
           │ Consumer Group               │ Consumer Group
           ▼                              ▼
┌──────────────────────┐      ┌──────────────────────┐
│     Worker Node 1    │      │     Worker Node 2    │
│  • Consume Tasks     │      │  • Consume Tasks     │
│  • Process Images    │      │  • Process Images    │
│  • Publish Results   │      │  • Publish Results   │
│  • Send Heartbeats   │      │  • Send Heartbeats   │
└──────────────────────┘      └──────────────────────┘
Data Flow
Upload: Client uploads image to Master node via web interface
Tile: Master splits image into 512x512 tiles
Distribute: Master publishes tiles to Kafka "tasks" topic
Consume: Workers consume tasks from their assigned partitions
Process: Workers apply transformations (grayscale, blur, etc.)
Publish: Workers publish processed tiles to "results" topic
Reconstruct: Master collects results and reconstructs final image
Monitor: Workers periodically send heartbeats to "heartbeats" topic
📦 Prerequisites
Hardware Requirements (per node)
2+ CPU cores
4GB+ RAM
10GB+ disk space
Network connectivity
Software Requirements
Operating System: Ubuntu 20.04+, Debian 10+, macOS, or Windows 10+
Python: 3.8 or higher
Java: OpenJDK 11 (for Kafka)
Network: ZeroTier VPN for distributed deployment (optional for local testing)
🚀 Installation
Step 1: Clone the Repository
git clone https://github.com/yourusername/distributed-image-processing.git
cd distributed-image-processing
Step 2: Set Up ZeroTier Network (For Distributed Deployment)
Install ZeroTier (all nodes)
# Ubuntu/Debian
curl -s https://install.zerotier.com | sudo bash

# macOS
brew install zerotier-one

# Windows: Download from https://www.zerotier.com/download/
Create Network (one person)
Go to https://my.zerotier.com
Sign up/Login
Click "Create A Network"
Note the Network ID (16-character hex string)
Join Network (all nodes)
sudo zerotier-cli join <NETWORK_ID>
sudo zerotier-cli listnetworks
Get Your IP
ip addr show zt0  # Linux/Mac
ipconfig          # Windows
Note IP addresses for configuration:

Master: e.g., 172.25.0.1
Kafka: e.g., 172.25.0.2
Worker 1: e.g., 172.25.0.3
Worker 2: e.g., 172.25.0.4
Step 3: Install Master Node (Node 1)
cd master
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
Update Configuration in master_node.py:

KAFKA_BROKER = "172.25.0.2:9092"  # Your Kafka broker IP
Step 4: Install Kafka Broker (Node 2)
cd broker

# Install Java
sudo apt update
sudo apt install -y openjdk-11-jdk

# Run setup script
chmod +x setup_kafka.sh
./setup_kafka.sh
The script will:

Download and install Kafka
Configure broker with your ZeroTier IP
Start Zookeeper and Kafka
Create required topics
Step 5: Install Worker Nodes (Node 3 & 4)
cd worker
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
For Worker 1 (Node 3), update in worker_node.py:

KAFKA_BROKER = "172.25.0.2:9092"
WORKER_ID = "worker_1"
WORKER_PORT = 8002
For Worker 2 (Node 4), update in worker_node.py:

KAFKA_BROKER = "172.25.0.2:9092"
WORKER_ID = "worker_2"
WORKER_PORT = 8003
⚙️ Configuration
Master Node Configuration
# master_node.py
KAFKA_BROKER = "172.25.0.2:9092"      # Kafka broker address
MIN_IMAGE_SIZE = 1024                  # Minimum image dimension
TILE_SIZE = 512                        # Tile size for processing
MASTER_PORT = 8000                     # Web server port
Kafka Broker Configuration
# broker/kafka_*/config/server.properties
broker.id=0
listeners=PLAINTEXT://172.25.0.2:9092
num.partitions=2                        # Default partitions (for 2 workers)
log.retention.hours=24                  # Message retention
message.max.bytes=10485760             # 10MB max message size
Worker Node Configuration
# worker_node.py
KAFKA_BROKER = "172.25.0.2:9092"
WORKER_ID = "worker_1"                 # Unique ID per worker
WORKER_PORT = 8002                     # Status endpoint port
HEARTBEAT_INTERVAL = 5                 # Heartbeat frequency (seconds)
🎮 Usage
Starting the System
1. Start Kafka Broker (Node 2)
cd broker/kafka_2.13-3.6.0

# Start Zookeeper
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

# Wait 10 seconds
sleep 10

# Start Kafka
bin/kafka-server-start.sh -daemon config/server.properties

# Verify topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
2. Start Master Node (Node 1)
cd master
source venv/bin/activate
python master_node.py
Access the dashboard at: http://localhost:8000 or http://172.25.0.1:8000

3. Start Worker Nodes (Node 3 & 4)
# On Node 3 (Worker 1)
cd worker
source venv/bin/activate
python worker_node.py

# On Node 4 (Worker 2)
cd worker
source venv/bin/activate
python worker_node.py
Check worker status:

Worker 1: http://localhost:8002/status
Worker 2: http://localhost:8003/status
Using the Web Interface
Open Dashboard: Navigate to http://<master-ip>:8000
Upload Image: Click upload area or drag-and-dropMinimum size: 1024x1024 pixels
Supported formats: PNG, JPEG, JPG
Select Transformation: Choose from dropdown menu
Start Processing: Click "Start Processing" button
Monitor Progress: Watch real-time progress bar and tile counts
View Workers: See active workers in the dashboard
Download Result: Click download button when complete
Using the API
Upload and Process Image
curl -X POST http://localhost:8000/process \
  -F "file=@image.png" \
  -F "transformation=grayscale"
Response:

{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "total_tiles": 16,
  "transformation": "grayscale",
  "status": "processing"
}
Check Job Progress
curl http://localhost:8000/job/<job_id>/progress
Response:

{
  "total": 16,
  "pending": 0,
  "completed": 16,
  "failed": 0
}
Download Result
curl http://localhost:8000/download/<job_id> -o result.png
Get Active Workers
curl http://localhost:8000/workers
Health Check
curl http://localhost:8000/health
🎨 Image Transformations
1. Grayscale
Converts image to grayscale (black and white).

transformation = "grayscale"
2. Blur
Applies Gaussian blur filter.

transformation = "blur"
3. Edge Detection
Detects edges using Sobel operator (OpenCV).

transformation = "edge_detection"
4. Sharpen
Sharpens image using convolution kernel.

transformation = "sharpen"
5. Rotate 90°
Rotates image 90 degrees clockwise.

transformation = "rotate_90"
📚 API Documentation
FastAPI provides automatic interactive API documentation:

Swagger UI: http://localhost:8000/docs
ReDoc: http://localhost:8000/redoc
🧪 Testing
Run Health Check
cd scripts
./check_system_health.sh
Run Performance Benchmark
cd scripts
./performance_benchmark.sh
Run Integration Tests
cd scripts
./integration_test.sh
Worker Comparison Test
cd scripts
./worker_comparison.sh
📊 Monitoring
Kafka Monitoring
cd broker/kafka_2.13-3.6.0

# List topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic tasks

# List consumer groups
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Describe consumer group
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group image-workers

# View messages
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic tasks --from-beginning --max-messages 5
Real-Time Kafka Dashboard
cd scripts
./kafka_dashboard.sh
Check Logs
# Master logs
tail -f master/master_node.log

# Worker logs
tail -f worker/worker_1.log
tail -f worker/worker_2.log

# Kafka logs
tail -f broker/kafka_*/logs/kafka.log
🐛 Troubleshooting
Kafka Won't Start
# Check if Zookeeper is running
ps aux | grep zookeeper

# Check logs
tail -100 broker/kafka_*/logs/kafka.log

# Clean up and restart
rm -rf /tmp/kafka-logs /tmp/zookeeper
cd broker/kafka_*/
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
sleep 10
bin/kafka-server-start.sh -daemon config/server.properties
Workers Not Consuming Tasks
# Check consumer group
cd broker/kafka_*/
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group image-workers

# Restart worker
pkill -f worker_node
python worker_node.py
Connection Issues
# Check ZeroTier connectivity
ping 172.25.0.2

# Check if Kafka is listening
netstat -tuln | grep 9092

# Allow firewall
sudo ufw allow 9092/tcp
sudo ufw allow 8000/tcp
sudo ufw allow 8002/tcp
sudo ufw allow 8003/tcp
Image Upload Fails
# Check image size
python3 -c "from PIL import Image; img = Image.open('image.png'); print(img.size)"

# Image must be at least 1024x1024
# Resize if needed using ImageMagick:
convert input.png -resize 1024x1024 output.png
📈 Performance
Benchmark Results
Image SizeWorkersTilesTimeThroughput1024x102414~12s0.33 tiles/s1024x102424~7s0.57 tiles/s2048x2048116~45s0.35 tiles/s2048x2048216~23s0.70 tiles/s4096x4096264~90s0.71 tiles/sKey Insight: 2 workers provide ~2x speedup, demonstrating effective parallelization.

🔐 Security Considerations
For production deployment, consider:

Kafka Authentication: Enable SASL/SCRAM
API Authentication: Add JWT or API key authentication
Input Validation: Already implemented for image size/format
Rate Limiting: Add rate limiting middleware
TLS/SSL: Encrypt all communication
Network Isolation: Use VPN or private network
🛠️ Technology Stack
Backend: Python 3.8+, FastAPI, Uvicorn
Message Broker: Apache Kafka 3.6.0
Image Processing: OpenCV, Pillow (PIL), NumPy
Database: SQLite
Real-time Communication: WebSockets
Kafka Client: confluent-kafka-python
Networking: ZeroTier VPN
📝 License
This project is licensed under the MIT License - see the LICENSE file for details.

👥 Authors
Person 1 - Master Node & Web Interface
Person 2 - Kafka Broker Setup & Management
Person 3 - Worker Node 1
Person 4 - Worker Node 2
🙏 Acknowledgments
Based on research paper: "Large Scale Image Processing in Real-time Environments with Kafka" by Kim & Jeong (2017)
Apache Kafka community for excellent documentation
FastAPI for the modern web framework
OpenCV for powerful image processing capabilities
Star ⭐ this repository if you find it helpful!
