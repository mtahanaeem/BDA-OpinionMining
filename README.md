# BDA Opinion Mining — Real-Time Sentiment Analysis Pipeline

A **Big Data Analytics** capstone project implementing an end-to-end opinion mining pipeline using Apache Kafka, PySpark Structured Streaming, Hugging Face Transformers, and Spark MLlib.

## Architecture

```
Sentiment140 Dataset (CSV)
        │
        ▼
  Kafka Producer ──► Kafka Topic ──► Spark Streaming Consumer
                                         │
                                    ┌────┴────┐
                                    │         │
                                    ▼         ▼
                              Hugging Face   Spark MLlib
                              API (Roberta)  (Logistic Regression)
                                    │         │
                                    └────┬────┘
                                         ▼
                                   Parquet Output
                                         │
                                         ▼
                              React / Vite Dashboard
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Message Broker | Apache Kafka (`kafka-python`) |
| Stream Processor | PySpark Structured Streaming 3.5 |
| Sentiment Inference | Hugging Face API (`cardiffnlp/twitter-roberta-base-sentiment`) |
| ML Training | Spark MLlib (Logistic Regression) |
| Frontend | React 19 + Vite 8 + Tailwind CSS 3.4 |
| Orchestration | Windows Batch (`run_all.bat`) |

## Quick Start

### Prerequisites
- Python 3.14+ (via `uv`)
- Node.js 20+
- Apache Kafka running on `localhost:9092`
- Hugging Face API token (set in `.env`)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/BDA-OpinionMining.git
cd BDA-OpinionMining

# 2. Create .env file with your Hugging Face token
echo "HF_API_TOKEN=your_token_here" > .env

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
2. **Launch Producer** — Streams Sentiment140 data to the `sentiment140` topic
3. **Launch Consumer** — Processes stream, calls Hugging Face API, saves Parquet output
4. **Train Model** — Runs Spark MLlib Logistic Regression on processed data
5. **View Dashboard** — Opens React app on `http://localhost:3000`

## Project Structure

```
├── producer.py                 # Kafka data producer
├── consumer_streaming.py       # Spark Streaming consumer + HF inference
├── train_evaluation_mllib.py   # Spark MLlib training & evaluation
├── run_all.bat                 # Windows pipeline orchestrator
├── requirements.txt            # Python dependencies
├── .env                        # API keys (excluded from git)
│
├── frontend/
│   ├── src/
│   │   ├── Dashboard.jsx       # Main dashboard component
│   │   ├── Dashboard.css       # Dashboard styles
│   │   ├── App.jsx             # Root app component
│   │   └── main.jsx            # React entry point
│   ├── package.json
│   └── vite.config.js
│
├── REPORT.md                   # Full project report
└── README.md
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `HF_API_TOKEN` | Hugging Face Inference API token | Yes |

## Authors

- **Muhammad Taha Naeem**
- **Suleman Ahmad**
- **Adil Hayat**

Department of Data Science — University of Central Punjab (UCP), Lahore  
6th Semester — Big Data Analytics (2025–26)
