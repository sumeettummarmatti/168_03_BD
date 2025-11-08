# Broker Node: Kafka Setup and Management

This folder contains the configuration and operational scripts for the Kafka Broker node in the Distributed Image Processing Pipeline project. The broker acts as the central messaging backbone for the entire system.

## Prerequisites

Before starting the broker, ensure the following are installed and configured on the machine:
1.  **Java 11 (OpenJDK)**
2.  **Apache Kafka 3.x.x** (Downloaded and extracted, e.g., in `/opt/kafka`)
3.  **ZeroTier Client** (Installed and joined to the team's network)
4.  **Firewall Ports Open:** Ports `9092` and `2181` must be open for TCP traffic.

## Configuration

The single most critical configuration is `advertised.listeners` in the `config/server.properties` file. This must be set to the broker machine's ZeroTier IP address to allow external connections from the Master and Worker nodes.

**Example:**
```properties
# file: config/server.properties

# This MUST be uncommented and set to the ZeroTier IP
advertised.listeners=PLAINTEXT://<YOUR_ZEROTIER_IP_HERE>:9092


## Execution Steps

(You can the run the commands given in broker_commands.txt in order or follow the steps given below.)

1.  **Start Zookeeper:**
    ```bash
    # Navigate to your Kafka installation directory
    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

2.  **Start Kafka Broker (in a new terminal):**
    ```bash
    ./bin/kafka-server-start.sh config/server.properties
    ```

3.  **Create Project Topics (in a new terminal):**
    Run the provided setup script (setup_topics.sh) to create the necessary topics with the correct partitioning.
    ```bash
    chmod +x setup_topics.sh
    ./setup_topics.sh
    ```


## Verification

To verify that the broker and topics are running correctly:
-   **Check running processes:** `jps -l` (should show `kafka.Kafka` and `QuorumPeerMain`)
-   **List topics:** `./bin/kafka-topics.sh --list --bootstrap-server localhost:9092` (should show `tasks`, `results`, `heartbeats`)