# ⚡ Real-Time Analytics (RTA) Virtual Lab

> **Interactive Virtual Laboratory for Real-Time Event Streaming, Cloud Analytics, and Edge AI.**  
> Designed following the **SRMIST Virtual Lab** pedagogy for the **Department of Electronics and Communication Engineering, SRM Institute of Science and Technology (SRM IST), Kattankulathur Campus**.  
> **Course / Lab Lead:** Dr. V. Ceronmani Sharmila (Associate Professor, ECE, SRM IST).

---

## 📖 Curriculum Architecture & Modules

The virtual lab is organized into three distinct, high-impact modules with fully executable sandboxes, live terminal simulators, circuit breadboards, and vision inference engines:

### 🔹 Module 1: Apache Kafka Distributed Systems & Event Streaming
Comprehensive hands-on labs with KRaft mode, multi-node clustering, and interactive simulations:

| # | Experiment Sandbox | Key Concepts & Execution Capabilities |
|---|---|---|
| **01** | **Single-Node KRaft Setup** (`exp-kraft.html`) | ZooKeeper-less Kafka 4.x setup, UUID generation, formatting log directories, and standalone broker launch in interactive CMD terminal. |
| **02** | **Core Messaging Pipeline** (`exp-pipeline.html`) | Distributed topic creation, console producer/consumer pipelines across 3 nodes, event serialization. |
| **02b** | **3-Machine Distributed Setup (Deep Manual)** (`exp-multi-machine.html`) | Detailed manual: Machine 1 (Broker/Controller: `192.168.1.10`), Machine 2 (Producer: `192.168.1.20`), Machine 3 (Consumer: `192.168.1.30`), `advertised.listeners`, and firewall rules. |
| **⭐** | **3-Machine Live Interactive Simulator** (`simulation.html`) | **Full Interactive Virtual Sandbox**: 3 synchronized live CMD terminals, Canvas network switch packet visualizer with glow animations, live commit log inspector, and network bug injector. |
| **03** | **Partitions & Consumer Groups** (`exp-partitions.html`) | Partition sharding, parallel consumption, consumer group rebalancing, and commit offset tracking (\(\text{Lag} = \text{LEO} - \text{CurrentOffset}\)). |
| **04** | **Cluster Fault Tolerance** (`exp-fault-tolerance.html`) | Multi-broker quorum resilience (RF=3), broker node crash simulation, ISR shrinkage, and automatic leader election. |
| **05** | **Python Programmatic Streaming** (`exp-python.html`) | In-browser IDE sandbox: Writing `KafkaProducer` and `KafkaConsumer` applications with `kafka-python`. |
| **06** | **Live Video Streaming Capstone** (`exp-video.html`) | OpenCV webcam capture, JPEG frame compression, streaming byte payloads across Kafka, and consumer video reconstruction. |

---

### 🔹 Module 2: Microsoft Azure Cloud Analytics (Prescribed Courseware)
Curriculum based on the 12 chapters of **Mastering Azure Analytics** with full execution capabilities:

| # | Chapter / Experiment | Executable Sandbox | Azure Technologies & Scope |
|---|---|---|---|
| **01 & 02** | **Enterprise Pipeline & Ingestion** | `exp-azure-pipeline.html` | 5-stage pipeline (Source &rarr; Ingest &rarr; Storage &rarr; Processing &rarr; Delivery), Lambda vs. Kappa, Event Hubs partition key distribution, AzCopy bulk migration simulator. |
| **03 & 04** | **Real-Time Stream Processing (ASA)** | `exp-azure-stream.html` | Interactive Stream Analytics T-SQL query editor, Tumbling, Hopping, and Sliding windows, live telemetry event feeder, alert output sink. |
| **05 & 06** | **Micro-Batching & Data Lake Batch** | `exp-azure-spark.html` | Interactive PySpark notebook environment, DStreams over RDDs, `updateStateByKey` stateful streaming, ADLS Gen2 directory tree browser. |
| **07 & 08** | **Synapse MPP & Speed Serving Layer** | `exp-azure-synapse.html` | Synapse SQL DW MPP architecture, Hash vs. Round-Robin distribution, DMS shuffle visualizer, PolyBase external lake scan, Redis Cache 0.4ms speed layer benchmark. |
| **09 & 12** | **Azure ML & Power BI Real-Time Dashboard** | `exp-azure-ml.html` | Boosted decision tree anomaly scoring, decision threshold tuning, live Power BI push dataset, gauges, trend charts, and fault alert simulator. |
| **⭐** | **Azure Cloud Console & Pipeline Simulator** | `azure-simulator.html` | **End-to-End Enterprise Operations Console**: Interactive Azure Cloud Shell / Azure CLI, live animated pipeline flow diagram, stage inspector, and traffic spike simulator. |

---

### 🔹 Module 3: NVIDIA Jetson Nano Edge AI Lab
Edge hardware setup, GPIO sensor interfaces, and local deep-learning inference on edge devices:

| # | Experiment Sandbox | Key Concepts & Execution Capabilities |
|---|---|---|
| **01** | **Hardware & OS Setup** (`exp-jetson-setup.html`) | Interactive virtual motherboard assembly (MicroSD slot, CSI camera ribbon, J48 power jumper 5W vs 10W MAXN), live Linux bash terminal with `cat /etc/nv_tegra_release`, `nvpmodel -q`, `tegrastats`, and GPU load inspection. |
| **02** | **IoT Sensor & 40-Pin GPIO** (`exp-jetson-gpio.html`) | Interactive breadboard circuit with animated green LED actuator on Pin 18, tactile push-button sensor on Pin 12, Python `Jetson.GPIO` code editor, and asynchronous interrupt handling. |
| **03** | **Edge Vision & Deep Learning** (`exp-jetson-vision.html`) | Real-time computer vision simulator: Multi-scene video viewport (SRM Campus Traffic, Pedestrians, Smart Factory), live AI bounding box detection overlay, model selector (YOLOv8, MobileNet-SSD, ResNet), TensorRT FP16/INT8 vs. PyTorch CPU benchmark (62 FPS vs 4.6 FPS). |

---

## 🚀 Running Locally

No server setup or npm packages are needed:

### Option 1: Direct Browser Launch
Open `index.html` directly in any modern browser (Chrome, Edge, Firefox).

### Option 2: Python HTTP Server
```bash
python -m http.server 8000
```
Then visit `http://localhost:8000`.

---

## 🌐 Deploying to GitHub Pages (Free Hosting)

1. Push this repository to: `https://github.com/CeronSharmila78/RTA`
2. Open your repository on GitHub and click **Settings** > **Pages**.
3. Under **Build and deployment > Source**, select **Deploy from a branch**.
4. Set branch to `main` and folder to `/(root)`.
5. Click **Save**. The website will be live at:
   ```
   https://ceronsharmila78.github.io/RTA/
   ```

---

## 📁 Repository Layout

```
├── index.html                   # Lab Portal (Modules 1, 2, and 3)
├── experiments.html             # Module 1: Kafka Curriculum Grid
├── simulation.html              # Module 1: 3-Machine Live Interactive Simulator & Visualizer
├── exp-multi-machine.html       # Module 1: 3-Machine Distributed Setup Detailed Manual
├── exp-kraft.html               # Module 1: Experiment 1 Sandbox (KRaft Setup)
├── exp-pipeline.html            # Module 1: Experiment 2 Sandbox (Core Pipeline)
├── exp-partitions.html          # Module 1: Experiment 3 Sandbox (Partitions & Offsets)
├── exp-fault-tolerance.html     # Module 1: Experiment 4 Sandbox (Resilience & ISRs)
├── exp-python.html              # Module 1: Experiment 5 Sandbox (Python Streaming)
├── exp-video.html               # Module 1: Experiment 6 Sandbox (OpenCV Video Capstone)
├── azure-lab-placeholder.html   # Module 2: Azure Analytics Lab (12 Experiments)
├── azure-simulator.html         # Module 2: Azure Cloud Analytics End-to-End Pipeline Simulator
├── exp-azure-pipeline.html      # Module 2: Experiment 1 & 2 Sandbox (Event Hubs & Ingestion)
├── exp-azure-stream.html        # Module 2: Experiment 4 Sandbox (Stream Analytics & Windows)
├── exp-azure-spark.html         # Module 2: Experiment 5 & 6 Sandbox (Spark & Data Lake)
├── exp-azure-synapse.html       # Module 2: Experiment 7 & 8 Sandbox (Synapse MPP & Serving)
├── exp-azure-ml.html            # Module 2: Experiment 9 & 12 Sandbox (Azure ML & Power BI)
├── jetson-nano-placeholder.html # Module 3: NVIDIA Jetson Nano Lab
├── exp-jetson-setup.html        # Module 3: Experiment 1 Sandbox (Hardware & OS Setup)
├── exp-jetson-gpio.html         # Module 3: Experiment 2 Sandbox (40-Pin GPIO & Breadboard)
├── exp-jetson-vision.html       # Module 3: Experiment 3 Sandbox (Edge Vision & TensorRT)
├── quiz.html & quiz-kafka.html  # Kafka Quizzes with score engine
├── quiz-azure.html              # Azure Module Quiz
├── quiz-jetson.html             # Jetson Nano Module Quiz
├── feedback.html                # Laboratory Feedback Form
├── css/
│   └── custom.css               # Custom Styles
└── js/
    ├── network-visualizer.js    # Canvas Network Switch Packet Visualizer
    ├── simulator.js             # Interactive Terminal Engine
    ├── simulator-cluster.js     # Broker Cluster Visualizer
    ├── simulator-python.js      # Python Streaming Simulator
    ├── simulator-video.js       # Video Streaming Canvas Engine
    └── progress-tracker.js      # Progress Tracking Engine
```

---

## 🎓 Academic Attribution

* **Author:** Dr. V. Ceronmani Sharmila
* **Designation:** Associate Professor
* **Department:** Electronics and Communication Engineering (ECE)
* **Institution:** SRM Institute of Science and Technology (SRM IST), Kattankulathur (KTR) Campus
