# ⚡ Real-Time Analytics (RTA) & Apache Kafka Virtual Lab

> **Interactive Virtual Laboratory for Real-Time Event Streaming, Multi-Machine Clusters, Cloud Analytics, IoT, and Edge AI.**  
> Designed following the **SRMIST Virtual Lab** pedagogy for the **Department of Electronics and Communication Engineering, SRM Institute of Science and Technology (SRM IST), Kattankulathur Campus**.  
> **Course / Lab Lead:** Dr. V. Ceronmani Sharmila (Associate Professor, ECE, SRM IST).

---

## 📖 Overview

This repository hosts the comprehensive **Real-Time Analytics (RTA) Distributed Systems Virtual Lab**. It integrates the standalone **Apache Kafka 3-Machine Multi-Node Virtual Lab** together with the full multi-module curriculum covering Apache Kafka KRaft architecture, live cluster simulators, Python streaming engines, OpenCV video streaming capstone, Azure cloud analytics, and Jetson Nano Edge AI.

Everything runs **100% client-side** using modern web technologies (HTML5, Tailwind CSS, Canvas particle animations, and JavaScript terminal emulators)—requiring zero backend servers or complicated setup to run or grade.

---

## 🧪 Experiments Catalog

### Module 1: Apache Kafka Distributed Event Streaming
| # | Experiment Sandbox | Key Concepts Covered |
|---|---|---|
| **01** | **Single-Node KRaft Setup** (`exp-kraft.html`) | ZooKeeper-less Kafka 4.x setup, UUID generation (`kafka-storage.bat random-uuid`), formatting storage directories, and standalone broker boot. |
| **02** | **Core Messaging Pipeline** (`exp-pipeline.html`) | Distributed topic creation, producer/consumer pipelines across 3 nodes, event serialization, and topic consumption. |
| **02b** | **3-Machine Distributed Setup (Deep Manual)** (`exp-multi-machine.html`) | Detailed manual: Machine 1 (Broker/Controller: `192.168.1.10`), Machine 2 (Producer: `192.168.1.20`), Machine 3 (Consumer: `192.168.1.30`), `advertised.listeners`, and firewall rules. |
| **⭐** | **3-Machine Live Interactive Simulator** (`simulation.html`) | **Full Interactive Virtual Sandbox**: 3 synchronized live CMD terminals, Canvas network switch packet visualizer with glow particle transmission, live commit log inspector, and network bug injector (`advertised.listeners` and Port 9092 blocks). |
| **03** | **Partitions & Consumer Groups** (`exp-partitions.html`) | Partition sharding, throughput scaling, consumer group rebalancing, and commit offset tracking (\(\text{Lag} = \text{LEO} - \text{CurrentOffset}\)). |
| **04** | **Cluster Fault Tolerance** (`exp-fault-tolerance.html`) | Multi-broker quorum resilience (RF=3), broker node crash simulation, ISR shrinkage, and automatic leader election. |
| **05** | **Python Programmatic Streaming** (`exp-python.html`) | In-browser IDE sandbox: Writing `KafkaProducer` and `KafkaConsumer` scripts using `kafka-python`. |
| **06** | **Live Video Streaming Capstone** (`exp-video.html`) | OpenCV webcam capture, JPEG frame compression, streaming byte payloads across Kafka, and real-time consumer playback. |

### Quizzes & Modules
- **Kafka Assessment Quizzes**: `quiz-kafka.html` and `quiz.html` (instant grading, confetti, and score breakdown)
- **Azure Analytics Module**: `azure-lab-placeholder.html` & `quiz-azure.html`
- **NVIDIA Jetson Nano Edge AI Module**: `jetson-nano-placeholder.html` & `quiz-jetson.html`
- **Student Feedback**: `feedback.html`

---

## 🌐 3-Machine Topology Architecture

```
                       ┌─────────────────────────┐
                       │   Virtual Switch (LAN)  │
                       └────────────┬────────────┘
                                    │
           ┌────────────────────────┼────────────────────────┐
           │                        │                        │
           ▼                        ▼                        ▼
┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
│  Machine 1: Broker   │ │ Machine 2: Producer  │ │ Machine 3: Consumer  │
│  192.168.1.10:9092   │ │     192.168.1.20     │ │     192.168.1.30     │
│ KRaft Controller:9093│ │ CLI / Python Producer│ │ CLI / Python Consumer│
└──────────────────────┘ └──────────────────────┘ └──────────────────────┘
```

---

## 🚀 Running Locally

No npm or external runtime is required:

### Option 1: Direct Browser Launch
Double click `index.html` to open in any web browser.

### Option 2: Python HTTP Server
```bash
# In the repository root:
python -m http.server 8000
```
Then visit `http://localhost:8000`.

---

## 🌐 Free Deployment via GitHub Pages

1. Push this repository to: `https://github.com/CeronSharmila78/RTA`
2. Open your repository on GitHub and click **Settings** > **Pages**.
3. Under **Build and deployment > Source**, select **Deploy from a branch**.
4. Set branch to `main` and folder to `/(root)`.
5. Click **Save**. The website will be live at:
   ```
   https://ceronsharmila78.github.io/RTA/
   ```

---

## 📁 Repository File Layout

```
├── index.html                   # Lab Portal Landing Page
├── experiments.html             # Kafka Curriculum Experiments Grid
├── simulation.html              # 3-Machine Live Interactive Terminal & Network Visualizer
├── exp-multi-machine.html       # 3-Machine Distributed Setup Detailed Lab Manual
├── exp-kraft.html               # Experiment 1 Sandbox (KRaft Single-Node Setup)
├── exp-pipeline.html            # Experiment 2 Sandbox (Core Pipeline)
├── exp-partitions.html          # Experiment 3 Sandbox (Partitions & Offsets)
├── exp-fault-tolerance.html     # Experiment 4 Sandbox (Resilience & ISRs)
├── exp-python.html              # Experiment 5 Sandbox (Python Streaming)
├── exp-video.html               # Experiment 6 Sandbox (OpenCV Video Capstone)
├── quiz-kafka.html              # Kafka Knowledge Assessment Quiz
├── quiz.html                    # Pre/Post-Lab Quiz
├── quiz-azure.html              # Azure Analytics Quiz
├── quiz-jetson.html             # Jetson Nano Quiz
├── azure-lab-placeholder.html   # Azure Analytics Lab Module
├── jetson-nano-placeholder.html # Jetson Nano Lab Module
├── feedback.html                # Laboratory Feedback Form
├── css/
│   └── custom.css               # Terminal & Card Styles
└── js/
    ├── network-visualizer.js    # Canvas Network Switch Packet Visualizer
    ├── simulator.js             # Terminal Engine & Packet Simulator
    ├── simulator-cluster.js     # Broker Cluster Visualizer
    ├── simulator-python.js      # Python Streaming Simulator
    ├── simulator-video.js       # Video Streaming Canvas Engine
    └── progress-tracker.js      # Milestone Tracking Engine
```

---

## 🎓 Academic Attribution

* **Author:** Dr. V. Ceronmani Sharmila
* **Designation:** Associate Professor
* **Department:** Electronics and Communication Engineering (ECE)
* **Institution:** SRM Institute of Science and Technology (SRM IST), Kattankulathur (KTR) Campus
