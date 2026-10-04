# Real-Time Analytics (RTA) Virtual Lab

> **Interactive Virtual Laboratory for Real-Time Event Streaming, Cloud Analytics, IoT, and Edge AI.**  
> Developed for **Department of Electronics and Communication Engineering, SRM Institute of Science and Technology (SRM IST), Kattankulathur Campus**.  
> **Course / Lab Lead:** Dr. V. Ceronmani Sharmila (Associate Professor, ECE, SRM IST).

---

## 📖 Overview

This repository hosts the **Real-Time Analytics (RTA) Distributed Systems Virtual Lab**. It provides a fully client-side, interactive split-screen sandbox platform containing comprehensive experiment theory, architectural diagrams, step-by-step procedures, and built-in interactive terminal/cluster simulators for Apache Kafka, Cloud Analytics, IoT, and Edge AI.

---

## 🧪 Experiments Included

| # | Experiment Sandbox | Description |
|---|---|---|
| **01** | **Single-Node KRaft Setup** (`exp-kraft.html`) | Initialize Apache Kafka 4.x without ZooKeeper. Generate cluster UUIDs, format KRaft metadata storage directories, and start standalone brokers. |
| **02** | **Core Messaging Pipeline** (`exp-pipeline.html`) | Configure multi-machine network listeners (`listeners` vs `advertised.listeners`), create distributed topics, and run CLI producers and consumers across nodes. |
| **03** | **Partitions & Consumer Groups** (`exp-partitions.html`) | Scale event throughput with partition sharding, parallel consumption, and automatic consumer group rebalances. |
| **04** | **Cluster Fault Tolerance** (`exp-fault-tolerance.html`) | Deploy 3-broker clusters with replication factor 3. Simulate broker failure, crash detection, ISR shrinkage, and dynamic leader re-election. |
| **05** | **Python Programmatic Streaming** (`exp-python.html`) | Program real-time producers and consumers with Python (`kafka-python`) and integrate streaming pipelines in an IDE sandbox. |
| **06** | **Live Video Streaming Capstone** (`exp-video.html`) | Capture webcam frames with OpenCV, compress JPEG byte streams, publish them to Kafka topics, and reconstruct live feeds on consumers. |
| **Bonus** | **Knowledge Assessment Quizzes** (`quiz-kafka.html`, `quiz-azure.html`, `quiz-jetson.html`) | Interactive multiple-choice assessments testing conceptual and practical understanding. |

---

## 🚀 Live Demo & Deployment

You can host this lab completely free via **GitHub Pages**:

1. Push this repository to `https://github.com/CeronSharmila78/RTA`.
2. Go to your repository **Settings** > **Pages**.
3. Under **Build and deployment** > **Source**, choose **Deploy from a branch**.
4. Set branch to `main` (or `master`) and directory to `/(root)`.
5. Click **Save**. Your site will be live at:
   ```
   https://ceronsharmila78.github.io/RTA/
   ```

---

## 💻 Local Quick Start

Since this is a lightweight static application (HTML5, Tailwind CSS, Vanilla JavaScript), no complex backend or node modules are required to run it locally:

### Option 1: Direct Browser Launch
Simply double-click `index.html` to open it in Google Chrome, Microsoft Edge, or Firefox.

### Option 2: Local HTTP Server
Using Python:
```bash
python -m http.server 8000
```
Then visit `http://localhost:8000` in your browser.

---

## 📁 Repository Structure

```
├── index.html                     # Virtual Lab Homepage & Overview
├── experiments.html               # Module 1: Kafka Experiments Catalog
├── exp-kraft.html                 # Experiment 1 Sandbox (KRaft Setup)
├── exp-pipeline.html              # Experiment 2 Sandbox (Core Pipeline)
├── exp-partitions.html            # Experiment 3 Sandbox (Partitions & Groups)
├── exp-fault-tolerance.html       # Experiment 4 Sandbox (Cluster Resilience)
├── exp-python.html                # Experiment 5 Sandbox (Python Streaming)
├── exp-video.html                 # Experiment 6 Sandbox (OpenCV Video Streaming)
├── quiz-kafka.html                # Kafka Knowledge Assessment Quiz
├── quiz-azure.html                # Azure Analytics Quiz Placeholder
├── quiz-jetson.html               # Jetson Nano Quiz Placeholder
├── azure-lab-placeholder.html     # Azure Analytics Lab Module
├── jetson-nano-placeholder.html   # Jetson Nano Edge AI Lab Module
├── feedback.html                  # Lab Feedback Form
├── css/
│   └── custom.css                 # Custom Styling & Typography
└── js/
    ├── progress-tracker.js        # Lab progress and step completion state
    ├── simulator.js               # Interactive CLI terminal emulator
    ├── simulator-cluster.js       # Multi-broker cluster topology visualizer
    ├── simulator-python.js        # Python code editor and execution engine
    └── simulator-video.js         # Video frame capture and streaming emulator
```

---

## 🎓 Academic Attribution

* **Author:** Dr. V. Ceronmani Sharmila
* **Designation:** Associate Professor
* **Department:** Electronics and Communication Engineering (ECE)
* **Institution:** SRM Institute of Science and Technology, Kattankulathur (KTR) Campus
