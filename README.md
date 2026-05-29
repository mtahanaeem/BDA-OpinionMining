# BDA-OpinionMining — Real-Time Sentiment Analysis Pipeline

[![Python](https://img.shields.io/badge/Python-3.14%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Hugging Face](https://img.shields.io/badge/Hugging%20Face-FFD21E?logo=huggingface&logoColor=000)](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=white)](https://react.dev/)
[![Vite](https://img.shields.io/badge/Vite-8-646CFF?logo=vite&logoColor=white)](https://vitejs.dev/)
[![Tailwind CSS](https://img.shields.io/badge/Tailwind%20CSS-3.4-06B6D4?logo=tailwind-css&logoColor=white)](https://tailwindcss.com/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

> **Big Data Analytics Capstone Project** — An end-to-end, real-time opinion mining pipeline that ingests social media streams via Apache Kafka, processes them with PySpark Structured Streaming, performs sentiment inference using Hugging Face Transformers (RoBERTa), trains a Spark MLlib classifier, and visualises results on a React dashboard.

---

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Project Structure](#-project-structure)
- [Environment Variables](#-environment-variables)
- [Authors](#-authors)

---

## 🏗 Architecture

```
                        ┌─────────────────────────────────────┐
                        │        Sentiment140 Dataset          │
                        │           (social_data.csv)          │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────────┐
                        │      Kafka Producer (producer.py)    │
                        │   Reads CSV, serialises to JSON,    │
                        │     publishes to 'social_sentiment' │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────────┐
                        │      Apache Kafka Broker Topic      │
                        │         social_sentiment            │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
                        ┌─────────────────────────────────────┐
                        │  Spark Structured Streaming Consumer │
                        │        (consumer_streaming.py)       │
                        │  ┌─ JSON deserialisation            │
                        │  ├─ Text cleaning (regex)           │
                        │  └─ Sentiment UDF invocation        │
                        └──────────────┬──────────────────────┘
                                       │
                         ┌─────────────┴─────────────┐
                         │                           │
                         ▼                           ▼
          ┌────────────────────────┐    ┌────────────────────────┐
          │  Hugging Face API      │    │    Spark MLlib         │
          │  (RoBERTa-based)       │    │  (Logistic Regression) │
          │  twitter-roberta-base- │    │  Batch training on     │
          │  sentiment             │    │  accumulated Parquet   │
          └──────────┬─────────────┘    └──────────┬─────────────┘
                     │                             │
                     └─────────────┬───────────────┘
                                   ▼
                    ┌─────────────────────────────────┐
                    │    Parquet Output Partitioned    │
                    │       by sentiment_label        │
                    └──────────────┬──────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────────┐
                    │   React + Vite Dashboard        │
                    │   (frontend/)                   │
                    └─────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Component             | Technology                                                              |
|-----------------------|-------------------------------------------------------------------------|
| **Message Broker**    | Apache Kafka via [`kafka-python`](https://pypi.org/project/kafka-python/) |
| **Stream Processing** | PySpark Structured Streaming 3.5                                        |
| **Sentiment Model**   | Hugging Face Inference API — [`cardiffnlp/twitter-roberta-base-sentiment`](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment) |
| **ML Training**       | Spark MLlib (Logistic Regression, Tokenizer, HashingTF)                 |
| **Dashboard**         | React 19 + Vite 8 + Tailwind CSS 3.4 + lucide-react                     |
| **Data Format**       | Parquet (partitioned by `sentiment_label`)                              |
| **Orchestration**     | Windows Batch Script (`run_all.bat`)                                    |
| **Package Manager**   | `uv` (Python), `npm` (Node.js)                                          |

The model classifies text into three sentiment categories:

| Label    | RoBERTa Mapping |
|----------|-----------------|
| Negative | `LABEL_0`       |
| Neutral  | `LABEL_1`       |
| Positive | `LABEL_2`       |

---

## ✨ Features

- **⏱ Real-Time Ingestion** — Kafka producer streams CSV data with a 200 ms delay, simulating a live social media firehose
- **🧹 Smart Text Cleaning** — PySpark UDF pipeline removes URLs, mentions, hashtags, and punctuation with regex
- **🤖 Transformer-Based Inference** — Hugging Face RoBERTa model returns per-tweet sentiment with confidence scores
- **📊 Hybrid ML Pipeline** — Compare cloud Transformer results against a local Spark MLlib Logistic Regression model
- **🗂 Partitioned Parquet Output** — Data persisted to disk partitioned by sentiment label for efficient querying
- **📈 Live Dashboard** — React + Vite frontend visualises streaming results
- **🔁 Fully Automated** — Single `run_all.bat` script launches every component in sequence
- **⏳ Graceful Degradation** — Consumer handles API timeouts, rate limits, and model loading delays with exponential back-off & retries

---

## 🚀 Quick Start

### Prerequisites

- Python 3.14+ (managed with [`uv`](https://docs.astral.sh/uv/))
- Node.js 20+
- Apache Kafka running on `localhost:9092`
- Hugging Face API token ([get one free](https://huggingface.co/settings/tokens))

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/mtahanaeem/BDA-OpinionMining.git
cd BDA-OpinionMining

# 2. Create .env file with your Hugging Face token
echo "HF_API_TOKEN=hf_your_token_here" > .env

# 3. Install Python dependencies
uv pip install -r requirements.txt

# 4. Install frontend dependencies
cd frontend
npm install
cd ..

# 5. Run the full pipeline
./run_all.bat
```

### Manual Steps

1. **Start Kafka** — Ensure Zookeeper and Kafka broker are running
2. **Launch Producer** — Streams `social_data.csv` to the `social_sentiment` topic:
   ```bash
   python producer.py
   ```
3. **Launch Consumer** — Processes stream, calls Hugging Face API, saves Parquet output:
   ```bash
   python consumer_streaming.py
   ```
4. **Train Model** — Runs Spark MLlib Logistic Regression on processed data:
   ```bash
   python train_evaluation_mllib.py
   ```
5. **View Dashboard** — Opens React app on `http://localhost:3000`:
   ```bash
   cd frontend && npm run dev
   ```

---

## 📁 Project Structure

```
├── producer.py                  # 📤 Kafka producer — reads CSV, publishes to topic
├── consumer_streaming.py        # 🔄 Spark Streaming consumer + HF sentiment inference
├── train_evaluation_mllib.py    # 🧠 Spark MLlib Logistic Regression training & evaluation
├── run_all.bat                  # ⚡ Windows pipeline orchestrator
├── requirements.txt             # 📦 Python dependencies
├── .env                         # 🔑 API keys (excluded from git)
│
├── frontend/                    # 🎨 React + Vite dashboard
│   ├── src/
│   │   ├── Dashboard.jsx        # Main dashboard component
│   │   ├── Dashboard.css        # Dashboard styles
│   │   ├── App.jsx              # Root app component
│   │   └── main.jsx             # React entry point
│   ├── package.json
│   └── vite.config.js
│
├── REPORT.md                    # 📄 Full project report
└── README.md                    # 📘 You are here
```

---

## 🔐 Environment Variables

| Variable       | Description                      | Required |
|----------------|----------------------------------|----------|
| `HF_API_TOKEN` | Hugging Face Inference API token | Yes      |

---

## 👥 Authors

<div align="center">

**Muhammad Taha Naeem** · **Suleman Ahmad** · **Adil Hayat**

Department of Data Science — University of Central Punjab (UCP), Lahore  
6th Semester — Big Data Analytics (2025–26)

</div>
