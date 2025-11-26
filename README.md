# Real-Time Narrative Intelligence Engine

A streaming data processing system that detects the birth of new news events in real-time, correlates them with social media posts, and tracks their spatiotemporal evolution using Apache Kafka, Apache Flink (PyFlink), and Elasticsearch.

## 🎯 Project Overview

This capstone project implements a **Real-Time Narrative Intelligence Engine** that:

1. **Ingests news** from NewsAPI and **simulates social media streams** (Twitter/Reddit)
2. **Detects new events** using semantic vectorization and clustering
3. **Correlates events with social posts** through stream-stream joins
4. **Enriches narratives** with Named Entity Recognition (NER) and sentiment analysis
5. **Stores results** in Elasticsearch for spatiotemporal analysis
6. **Visualizes insights** using Tableau dashboards

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION                                │
│  NewsAPI → news_producer.py → Kafka (raw_news)                  │
│  Faker → social_producer.py → Kafka (raw_social)                │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│              FLINK JOB 1: Event Birth Detector                   │
│  • Reads from raw_news                                           │
│  • Vectorizes headlines (SentenceTransformer)                    │
│  • Maintains cluster centroids (Stateful)                        │
│  • Detects NEW events (cosine similarity < threshold)            │
│  • Emits to detected_events                                      │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│           FLINK JOB 2: Narrative Entity Tracker                  │
│  • Stream-Stream Join:                                           │
│    - Input 1: detected_events (broadcast)                        │
│    - Input 2: raw_social (high velocity)                         │
│  • Match keywords from events to social posts                    │
│  • Extract entities (spaCy NER)                                  │
│  • Analyze sentiment (VADER)                                     │
│  • Sink enriched docs to Elasticsearch                           │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                    ELASTICSEARCH                                 │
│  Index: enriched_narratives                                      │
│  • event_id, keywords, matched social posts                      │
│  • entities (persons, orgs, locations)                           │
│  • sentiment scores                                              │
│  • spatiotemporal data (location, timestamp)                     │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                     TABLEAU DASHBOARD                            │
│  • Event timeline visualization                                  │
│  • Sentiment trends                                              │
│  • Geographic heatmaps                                           │
│  • Entity networks                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
narrative_engine/
├── docker-compose.yml          # Infrastructure setup
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── src/
    ├── config/
    │   ├── __init__.py
    │   └── config.py           # Configuration parameters
    ├── ingestion/
    │   ├── __init__.py
    │   ├── news_producer.py    # NewsAPI → Kafka
    │   └── social_producer.py  # Fake social posts → Kafka
    └── processing/
        ├── __init__.py
        ├── utils.py            # NLP utilities
        ├── job_event_detector.py    # Flink Job 1
        └── job_entity_tracker.py    # Flink Job 2
```

---

## 🚀 Installation & Setup

### 1. Prerequisites

- **Docker** and **Docker Compose** installed
- **Python 3.8+**
- **Git** (optional, for version control)

### 2. Clone/Create Project Directory

```bash
cd narrative_engine
```

### 3. Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm
```

### 4. Configure NewsAPI Key

Edit `src/config/config.py`:

```python
NEWS_API_KEY = 'your_actual_api_key_here'  # Get from https://newsapi.org/
```

### 5. Start Infrastructure with Docker

```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

**Services:**
- Zookeeper: `localhost:2181`
- Kafka: `localhost:9092`
- Flink JobManager: `localhost:8081` (Web UI)
- Flink TaskManager: (2 instances)
- Elasticsearch: `localhost:9200`

### 6. Create Kafka Topics

```bash
# Access Kafka container
docker exec -it narrative-kafka bash

# Create topics
kafka-topics --create --topic raw_news --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic raw_social --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic detected_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics --create --topic enriched_narratives --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify topics
kafka-topics --list --bootstrap-server localhost:9092

# Exit container
exit
```

---

## 🎬 Running the System

### Step 1: Start Data Producers

**Terminal 1 - News Producer:**
```bash
cd src/ingestion
python news_producer.py
```

This will:
- Fetch news from NewsAPI every 2 minutes
- Deduplicate articles by URL
- Produce to `raw_news` Kafka topic

**Terminal 2 - Social Media Producer:**
```bash
cd src/ingestion
python social_producer.py
```

This will:
- Generate 10 social media posts per second
- 20% contain event-related keywords
- Produce to `raw_social` Kafka topic

### Step 2: Submit Flink Jobs

**Terminal 3 - Event Detector Job:**
```bash
# Copy job to Flink container
docker cp src/processing/job_event_detector.py narrative-flink-jobmanager:/opt/flink/usrlib/
docker cp src/processing/utils.py narrative-flink-jobmanager:/opt/flink/usrlib/
docker cp src/config/config.py narrative-flink-jobmanager:/opt/flink/usrlib/

# Submit job
docker exec narrative-flink-jobmanager flink run -py /opt/flink/usrlib/job_event_detector.py
```

**Terminal 4 - Narrative Tracker Job:**
```bash
# Copy job to Flink container
docker cp src/processing/job_entity_tracker.py narrative-flink-jobmanager:/opt/flink/usrlib/

# Submit job
docker exec narrative-flink-jobmanager flink run -py /opt/flink/usrlib/job_entity_tracker.py
```

### Step 3: Monitor Progress

**Flink Web UI:**
- Open: http://localhost:8081
- View running jobs, task managers, checkpoints

**Kafka Topics:**
```bash
# Check raw_news topic
docker exec narrative-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_news --from-beginning --max-messages 5

# Check detected_events topic
docker exec narrative-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic detected_events --from-beginning --max-messages 5
```

**Elasticsearch:**
```bash
# Check index
curl http://localhost:9200/enriched_narratives/_search?pretty

# Count documents
curl http://localhost:9200/enriched_narratives/_count
```

---

## 📊 Tableau Integration

### 1. Install Elasticsearch JDBC Driver

Download from: https://www.elastic.co/downloads/jdbc-client

### 2. Connect Tableau to Elasticsearch

1. Open Tableau Desktop
2. **Connect to Data** → **More...** → **Elasticsearch (via JDBC)**
3. **Server:** `localhost`
4. **Port:** `9200`
5. **Index:** `enriched_narratives`

### 3. Create Visualizations

**Timeline Chart:**
- X-axis: `timestamp`
- Y-axis: Count of documents
- Color: `sentiment` (Red = negative, Green = positive)

**Geographic Heatmap:**
- Geography: `location`
- Size: Count of posts
- Color: Average `sentiment`

**Entity Network:**
- Dimension: `entities.persons`
- Connected to: `event_id`

**Sentiment Trend:**
- Line chart: `timestamp` vs `sentiment`
- Filter: `event_id`

---

## 🔧 Configuration

### Key Parameters in `src/config/config.py`:

```python
# Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_RAW_NEWS = 'raw_news'
TOPIC_RAW_SOCIAL = 'raw_social'
TOPIC_DETECTED_EVENTS = 'detected_events'

# Elasticsearch
ELASTICSEARCH_HOST = 'http://localhost:9200'
ES_INDEX_ENRICHED = 'enriched_narratives'

# NewsAPI
NEWS_API_KEY = 'YOUR_KEY_HERE'
NEWS_FETCH_INTERVAL = 120  # seconds

# Social Media Simulation
SOCIAL_POSTS_PER_SECOND = 10

# Event Detection
COSINE_SIMILARITY_THRESHOLD = 0.75
NEW_EVENT_THRESHOLD = 0.75  # Lower = more sensitive

# NLP Models
EMBEDDING_MODEL = 'sentence-transformers/all-MiniLM-L6-v2'
SPACY_MODEL = 'en_core_web_sm'
```

---

## 🧪 Testing

### Test Individual Components

**Test News Producer:**
```bash
cd src/ingestion
python news_producer.py
# Should see articles being fetched and sent to Kafka
```

**Test Social Producer:**
```bash
cd src/ingestion
python social_producer.py
# Should see 10 posts/second generated
```

**Test NLP Utils:**
```bash
cd src/processing
python -c "from utils import *; model = load_embedding_model(); print(model.encode('test'))"
```

### Verify Data Flow

```bash
# Monitor Kafka topics
docker exec narrative-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_news
docker exec narrative-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic detected_events

# Query Elasticsearch
curl "http://localhost:9200/enriched_narratives/_search?pretty&size=1"
```

---

## 🐛 Troubleshooting

### Issue: Flink jobs fail to start

**Solution:**
```bash
# Check Flink logs
docker logs narrative-flink-jobmanager
docker logs narrative-flink-taskmanager

# Ensure Python dependencies are installed in Flink container
docker exec narrative-flink-jobmanager pip install -r /opt/flink/usrlib/requirements.txt
```

### Issue: No events detected

**Possible causes:**
- Threshold too high (lower `NEW_EVENT_THRESHOLD` in config)
- Not enough news articles (check news producer)
- Model not loaded (check Flink logs)

### Issue: No matches between events and social posts

**Solution:**
- Ensure `EVENT_KEYWORDS` in config match news topics
- Check that 20% of social posts contain keywords
- Verify both Flink jobs are running

### Issue: Elasticsearch connection refused

**Solution:**
```bash
# Check Elasticsearch health
curl http://localhost:9200/_cluster/health

# Restart Elasticsearch
docker-compose restart elasticsearch
```

---

## 📈 Performance Tuning

### Increase Throughput

1. **Kafka partitions:**
   ```bash
   kafka-topics --alter --topic raw_social --partitions 6 --bootstrap-server localhost:9092
   ```

2. **Flink parallelism:**
   Edit `src/config/config.py`:
   ```python
   FLINK_PARALLELISM = 4
   ```

3. **Elasticsearch bulk size:**
   ```python
   .set_bulk_flush_max_actions(500)
   ```

### Reduce Memory Usage

1. **Limit state TTL:**
   ```python
   FLINK_STATE_TTL_HOURS = 12  # Reduce from 24
   ```

2. **Reduce social post rate:**
   ```python
   SOCIAL_POSTS_PER_SECOND = 5  # Reduce from 10
   ```

---

## 🛑 Shutdown

```bash
# Stop producers (Ctrl+C in terminals)

# Cancel Flink jobs
docker exec narrative-flink-jobmanager flink cancel <job-id>

# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v
```

---

## 📚 Additional Resources

- **Apache Flink Docs:** https://flink.apache.org/
- **PyFlink Guide:** https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/overview/
- **NewsAPI Docs:** https://newsapi.org/docs
- **Elasticsearch Docs:** https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
- **Tableau Elasticsearch:** https://help.tableau.com/current/pro/desktop/en-us/examples_elasticsearch.htm

---

## 🎓 Learning Outcomes

This project demonstrates:

1. **Stream Processing:** Real-time data pipelines with Apache Flink
2. **Stateful Computation:** Maintaining cluster centroids across streaming data
3. **Stream-Stream Joins:** Correlating high-velocity streams
4. **NLP in Production:** Vectorization, NER, sentiment at scale
5. **Event Detection:** Birth of new narratives from news
6. **Spatiotemporal Analysis:** Tracking events across space and time

---

## 📝 License

This project is for educational purposes. Ensure you comply with NewsAPI terms of service.

---

## 👨‍💻 Author

Created as a capstone project for demonstrating real-time stream processing with Apache Flink and NLP.

---

**Happy Streaming! 🚀**
