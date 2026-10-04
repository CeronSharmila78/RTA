# ⚡ Real-Time Analytics (RTA) Virtual Lab

> **Interactive Virtual Laboratory for Real-Time Event Streaming, Cloud Analytics, and Edge AI.**  
> Designed following the **SRMIST Virtual Lab** pedagogy for the **Department of Electronics and Communication Engineering, SRM Institute of Science and Technology (SRM IST), Kattankulathur Campus**.  
> **Course / Lab Lead:** Dr. V. Ceronmani Sharmila (Associate Professor, ECE, SRM IST).

---

## 📖 Curriculum Architecture & Modules

The virtual lab is organized into three distinct, high-impact modules:

### 🔹 Module 1: Apache Kafka Distributed Systems & Event Streaming
Comprehensive hands-on labs with KRaft mode, multi-node clustering, and interactive simulations:

| # | Experiment Sandbox | Key Concepts Covered |
|---|---|---|
| **01** | **Single-Node KRaft Setup** (`exp-kraft.html`) | ZooKeeper-less Kafka 4.x setup, UUID generation, formatting log directories, and standalone broker launch. |
| **02** | **Core Messaging Pipeline** (`exp-pipeline.html`) | Distributed topic creation, console producer/consumer pipelines across 3 nodes, event serialization. |
| **02b** | **3-Machine Distributed Setup (Deep Manual)** (`exp-multi-machine.html`) | Detailed manual: Machine 1 (Broker/Controller: `192.168.1.10`), Machine 2 (Producer: `192.168.1.20`), Machine 3 (Consumer: `192.168.1.30`), `advertised.listeners`, and firewall rules. |
| **⭐** | **3-Machine Live Interactive Simulator** (`simulation.html`) | **Full Interactive Virtual Sandbox**: 3 synchronized live CMD terminals, Canvas network switch packet visualizer with glow animations, live commit log inspector, and network bug injector. |
| **03** | **Partitions & Consumer Groups** (`exp-partitions.html`) | Partition sharding, parallel consumption, consumer group rebalancing, and commit offset tracking (\(\text{Lag} = \text{LEO} - \text{CurrentOffset}\)). |
| **04** | **Cluster Fault Tolerance** (`exp-fault-tolerance.html`) | Multi-broker quorum resilience (RF=3), broker node crash simulation, ISR shrinkage, and automatic leader election. |
| **05** | **Python Programmatic Streaming** (`exp-python.html`) | In-browser IDE sandbox: Writing `KafkaProducer` and `KafkaConsumer` applications with `kafka-python`. |
| **06** | **Live Video Streaming Capstone** (`exp-video.html`) | OpenCV webcam capture, JPEG frame compression, streaming byte payloads across Kafka, and consumer video reconstruction. |

---

### 🔹 Module 2: Microsoft Azure Cloud Analytics (Prescribed Courseware)
Curriculum based on the 12 chapters of **Mastering Azure Analytics**:

| # | Chapter / Experiment | Azure Technologies & Scope |
|---|---|---|
| **01** | **Enterprise Analytics & Pipeline Design** | 5-stage pipeline (Source &rarr; Ingest &rarr; Storage &rarr; Processing &rarr; Delivery), Lambda vs. Kappa architectures. |
| **02** | **Data Ingestion (Bulk & Streaming)** | Ingest Loading Layer, AzCopy, Azure Import/Export with WAImportExport, and Event Hubs with partition keys. |
| **03** | **Storing Data in Azure** | Hierarchical namespace in ADLS Gen2, Blob Storage Hot/Cool/Archive tiers, and replication (LRS/ZRS/GRS). |
| **04** | **Real-Time Processing** | Azure Stream Analytics (ASA) with Tumbling, Hopping, Sliding windows, and Apache Storm on HDInsight. |
| **05** | **Real-Time Micro-Batching** | Spark Streaming DStreams over RDDs on HDInsight, stateful transformations with `updateStateByKey`. |
| **06** | **Batch Processing** | Hadoop MapReduce and Apache Hive on HDInsight, Azure Data Lake Analytics (ADLA) with U-SQL. |
| **07** | **Interactive Querying** | Azure SQL Data Warehouse (Synapse) MPP architecture, Hash/Replicated distributions, and PolyBase. |
| **08** | **Serving Layer (Hot & Cold Paths)** | Sub-millisecond Speed Layer delivery with Azure Redis Cache, global distribution and 5 consistency levels in Cosmos DB. |
| **09** | **Intelligence & Machine Learning** | Azure Machine Learning Studio, predictive experiments, R Server on HDInsight, and Cognitive Services APIs. |
| **10** | **Managing Metadata & Governance** | Avoiding the "Data Swamp", data asset registration, schema discovery, and Azure Data Catalog. |
| **11** | **Data Protection & Security** | Defense-in-depth, Azure Active Directory (AAD), Role-Based Access Control (RBAC), POSIX ACLs in ADLS, and Key Vault. |
| **12** | **Performing Analytics with Power BI** | Power BI Desktop dashboards, real-time push-streaming datasets, and end-to-end enterprise reporting. |

---

### 🔹 Module 3: NVIDIA Jetson Nano Edge AI Lab
Edge hardware setup, GPIO sensor interfaces, and local deep-learning inference on edge devices (`jetson-nano-placeholder.html` & `quiz-jetson.html`).

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
├── simulation.html              # 3-Machine Live Interactive Simulator & Visualizer
├── exp-multi-machine.html       # 3-Machine Distributed Setup Detailed Manual
├── exp-kraft.html               # Experiment 1 Sandbox (KRaft Setup)
├── exp-pipeline.html            # Experiment 2 Sandbox (Core Pipeline)
├── exp-partitions.html          # Experiment 3 Sandbox (Partitions & Offsets)
├── exp-fault-tolerance.html     # Experiment 4 Sandbox (Resilience & ISRs)
├── exp-python.html              # Experiment 5 Sandbox (Python Streaming)
├── exp-video.html               # Experiment 6 Sandbox (OpenCV Video Capstone)
├── azure-lab-placeholder.html   # Module 2: Azure Analytics Lab (12 Experiments)
├── jetson-nano-placeholder.html # Module 3: NVIDIA Jetson Nano Lab
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
