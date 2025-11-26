# 🚀 Real-Time Narrative Intelligence Platform

## ⚡ Quick Start (5 Minutes)

### 1️⃣ Prerequisites

- **Python 3.8+** - [Download](https://www.python.org/downloads/)
- **Docker Desktop** - [Download](https://www.docker.com/products/docker-desktop/) (4GB RAM minimum)
- **NewsAPI Key** - [Get Free Key](https://newsapi.org/register) (100 requests/day)

---

## 📥 Setup Steps

### Step 1: Extract Project
```bash
# Extract ZIP file and navigate to folder
cd SPA
```

### Step 2: Install Python Packages

**Windows:**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### Step 3: Configure API Key

Create `config.py` in root folder:
```python
NEWSAPI_KEY = "your_api_key_here"  # From https://newsapi.org
```

### Step 4: Start Docker Services
```bash
docker-compose up -d
# Wait 30 seconds for initialization
```

Verify services:
- Elasticsearch: http://localhost:9200
- Kibana: http://localhost:5601
- Kafka-UI: http://localhost:9000

---

## ▶️ Run the System

**Option 1 - Quick Start (Recommended):**
```bash
.\venv\Scripts\Activate.ps1  # Windows
source venv/bin/activate     # Linux/Mac
python start_pipeline.py
```

**Option 2 - Manual Start (3 terminals):**

Terminal 1 - Producer:
```bash
.\venv\Scripts\Activate.ps1  # Windows
python kafka_producer.py
```

Terminal 2 - Consumer:
```bash
.\venv\Scripts\Activate.ps1  # Windows
python elasticsearch_consumer.py
```

Terminal 3 - Dashboard:
```bash
.\venv\Scripts\Activate.ps1  # Windows
cd narrative_engine
streamlit run dashboard_enhanced.py --server.port 8501
```

**Access Dashboard:** http://localhost:8501

### Dashboard Features:
- **Data Source:** Select "Elasticsearch (Live)" or "CSV File" in sidebar
- **6 Tabs:** HDBSCAN Clustering, AI Summaries, Geospatial Maps, Temporal Analysis, Entity Networks, Live Feed

---

## 🛑 Stop the System

```bash
# Press Ctrl+C in all terminals, then:
docker-compose down
```

---

## ⚠️ Troubleshooting

**Docker not starting?**
```bash
docker-compose down -v
docker-compose up -d --force-recreate
```

**Port already in use?**
```bash
netstat -ano | findstr :9200  # Find process
taskkill /PID <process_id> /F  # Kill it
```

**NewsAPI rate limit?**
- Use CSV data source in dashboard sidebar instead

**Dashboard shows no data?**
1. Switch to "CSV File (Hybrid Dataset)" in sidebar
2. Or wait for producer to fetch articles (check terminal)

**Virtual environment issues?**
```bash
Remove-Item -Recurse -Force venv  # Windows
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

Check logs: `docker-compose logs elasticsearch` or `docker-compose logs kafka`

---

**That's it! Run `python start_pipeline.py` and open http://localhost:8501** 🚀
