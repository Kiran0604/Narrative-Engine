"""
🚀 Real-Time Narrative Intelligence Dashboard
Advanced ML-Powered Geospatial & Sentiment Analysis Platform
- Live Auto-Refreshing Data Stream
- HDBSCAN Adaptive Clustering
- Sentence Transformers Embeddings  
- BART AI Summarization
- Real-Time Geospatial Analysis with spaCy NER
- Advanced Temporal Lifecycle Tracking
- Interactive Network Graphs
"""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import Counter
import json
import numpy as np
import warnings
import folium
from streamlit_folium import st_folium
import spacy
from sentence_transformers import SentenceTransformer
import hdbscan
from transformers import pipeline
import torch
import time

warnings.filterwarnings('ignore')

# ============= ADVANCED ANALYTICS FUNCTIONS =============

def calculate_hype_score(text, entities):
    """
    Calculate Hype vs. Substance Index (Clickbait Detection)
    High sensationalism + Low entity density = Clickbait
    """
    if not text or len(text) < 10:
        return 0, 0, 0
    
    # Sensationalism indicators
    sensational_words = ['breaking', 'shocking', 'unbelievable', 'must-see', 'you won\'t believe', 
                         'secret', 'exposed', 'revealed', 'scandal', 'explosive', 'urgent']
    emotional_words = ['amazing', 'terrible', 'incredible', 'awful', 'stunning', 'horrific']
    
    text_lower = text.lower()
    
    # Calculate sensationalism score
    sensational_count = sum(1 for word in sensational_words if word in text_lower)
    emotional_count = sum(1 for word in emotional_words if word in text_lower)
    exclamation_count = text.count('!')
    caps_ratio = sum(1 for c in text if c.isupper()) / len(text) if len(text) > 0 else 0
    
    sensationalism = (sensational_count * 2 + emotional_count + exclamation_count + caps_ratio * 5)
    
    # Calculate entity density (factual substance)
    word_count = len(text.split())
    entity_count = len(entities) if isinstance(entities, list) else 0
    entity_density = (entity_count / word_count * 10) if word_count > 0 else 0
    
    # Hype Score: High sensationalism + Low entity density = High hype
    hype_score = max(0, min(10, sensationalism - entity_density))
    
    return hype_score, sensationalism, entity_density

def calculate_narrative_drift(old_embeddings, new_embeddings):
    """
    Calculate narrative drift by measuring centroid movement in vector space
    Detects when story meaning shifts over time
    """
    if old_embeddings is None or new_embeddings is None:
        return 0, None, None
    
    try:
        old_centroid = np.mean(old_embeddings, axis=0)
        new_centroid = np.mean(new_embeddings, axis=0)
        
        # Cosine distance
        from numpy.linalg import norm
        cosine_sim = np.dot(old_centroid, new_centroid) / (norm(old_centroid) * norm(new_centroid))
        drift = 1 - cosine_sim  # 0 = no drift, 1 = complete drift
        
        return drift, old_centroid, new_centroid
    except:
        return 0, None, None

def extract_entity_relationships(df, nlp):
    """
    Extract entity co-occurrence and sentiment relationships
    Creates network edges between entities that appear together
    """
    relationships = []
    
    for idx, row in df.iterrows():
        entities = row.get('entities', [])
        sentiment = row.get('sentiment', 'neutral')
        sentiment_score = row.get('sentiment_score', 0)
        
        if not isinstance(entities, list) or len(entities) < 2:
            continue
        
        # Create edges for all entity pairs in same document
        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                relationships.append({
                    'source': entities[i],
                    'target': entities[j],
                    'sentiment': sentiment,
                    'sentiment_score': sentiment_score,
                    'weight': 1
                })
    
    return relationships

def create_entity_network_graph(relationships):
    """
    Create interactive network graph showing entity relationships
    Edge color shows sentiment between entities
    """
    if not relationships:
        return None
    
    # Aggregate relationships
    from collections import defaultdict
    edges = defaultdict(lambda: {'weight': 0, 'sentiments': []})
    
    for rel in relationships:
        key = tuple(sorted([rel['source'], rel['target']]))
        edges[key]['weight'] += rel['weight']
        edges[key]['sentiments'].append(rel['sentiment_score'])
    
    # Build graph data
    edge_traces = []
    node_set = set()
    
    for (source, target), data in edges.items():
        node_set.add(source)
        node_set.add(target)
        
        avg_sentiment = np.mean(data['sentiments']) if data['sentiments'] else 0
        
        # Color based on sentiment
        if avg_sentiment > 0.2:
            color = 'green'
        elif avg_sentiment < -0.2:
            color = 'red'
        else:
            color = 'gray'
        
        edge_traces.append({
            'source': source,
            'target': target,
            'weight': data['weight'],
            'sentiment': avg_sentiment,
            'color': color
        })
    
    return edge_traces, list(node_set)

# Load ML models (cached for performance)
@st.cache_resource
def load_ml_models():
    """Load all ML models once and cache them"""
    try:
        # Sentence transformer for embeddings
        embedder = SentenceTransformer('all-MiniLM-L6-v2')
        
        # spaCy for NER
        nlp = spacy.load('en_core_web_sm')
        
        # BART for summarization (use smaller model for speed)
        summarizer = pipeline("summarization", model="facebook/bart-large-cnn", 
                            device=0 if torch.cuda.is_available() else -1)
        
        return embedder, nlp, summarizer
    except Exception as e:
        st.error(f"Error loading models: {e}")
        return None, None, None

# Page configuration
st.set_page_config(
    page_title="Enhanced Narrative Intelligence",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with enhanced animations
st.markdown("""
    <style>
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    @keyframes slideIn {
        from { transform: translateX(-20px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    .stApp { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px; border-radius: 10px; color: white;
        animation: slideIn 0.5s ease-out;
    }
    .stMetric {
        background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%);
        padding: 20px; border-radius: 12px;
        border: 1px solid #3b82f6;
        box-shadow: 0 4px 6px rgba(59, 130, 246, 0.2);
        transition: transform 0.3s;
    }
    .stMetric:hover {
        transform: scale(1.05);
        box-shadow: 0 8px 12px rgba(59, 130, 246, 0.4);
    }
    .stMetric label { color: #93c5fd !important; font-weight: 600; }
    .stMetric [data-testid="stMetricValue"] { color: #ffffff !important; font-size: 2rem; }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
        border-right: 1px solid #334155;
    }
    h1 { 
        color: #f1f5f9 !important;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.5);
        animation: slideIn 0.8s ease-out;
    }
    h2, h3 { color: #f1f5f9 !important; }
    .streamlit-expanderHeader {
        background-color: #1e293b; border-radius: 8px; border: 1px solid #334155;
        transition: all 0.3s;
    }
    .streamlit-expanderHeader:hover {
        border-color: #3b82f6;
        box-shadow: 0 4px 8px rgba(59, 130, 246, 0.2);
    }
    .stButton>button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white; border: none; border-radius: 8px;
        padding: 10px 24px; font-weight: 600; transition: all 0.3s;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 20px rgba(59, 130, 246, 0.4);
    }
    .live-indicator {
        display: inline-block;
        width: 12px; height: 12px;
        background: #10b981;
        border-radius: 50%;
        animation: pulse 2s infinite;
        margin-right: 8px;
    }
    .objective-box {
        background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%);
        padding: 15px; border-radius: 8px; margin: 10px 0;
        border-left: 4px solid #3b82f6;
    }
    .summary-box {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        padding: 15px; border-radius: 8px; margin: 10px 0;
        border-left: 4px solid #10b981; color: white;
    }
    </style>
""", unsafe_allow_html=True)

# Configuration
ES_URL = "http://localhost:9200"
ES_INDEX = "news_articles"  # Updated to use correct index

# Sidebar controls for real-time updates
with st.sidebar:
    st.markdown("## 📁 Data Source")
    
    data_source = st.radio(
        "Select Data Source",
        ["Elasticsearch (Live)", "CSV File (Hybrid Dataset)"],
        help="Choose between live Elasticsearch or CSV dataset"
    )
    
    if data_source == "CSV File (Hybrid Dataset)":
        csv_path = st.text_input(
            "CSV File Path",
            value="../exports/tableau_hybrid_clean.csv",
            help="Path to hybrid dataset CSV"
        )
    
    st.markdown("---")
    st.markdown("## ⚙️ Dashboard Controls")
    
    auto_refresh = st.toggle("🔴 Live Mode", value=True if data_source == "Elasticsearch (Live)" else False, help="Auto-refresh every 10 seconds")
    refresh_interval = st.slider("Refresh Interval (seconds)", 5, 60, 10)
    
    st.markdown("---")
    st.markdown("## 📊 Data Filters")
    data_limit = st.selectbox("Data Points", [500, 1000, 2000, 5000], index=2)
    
    st.markdown("---")
    if st.button("🔄 Force Refresh Now"):
        st.cache_data.clear()
        st.rerun()

# Data fetching functions with shorter cache for real-time
@st.cache_data(ttl=5)
def get_elasticsearch_stats():
    try:
        response = requests.get(f"{ES_URL}/{ES_INDEX}/_count", timeout=5)
        return response.json().get('count', 0) if response.status_code == 200 else 0
    except: return 0

@st.cache_data(ttl=5)
def get_elasticsearch_data(size=2000):
    try:
        query = {
            "size": size,
            "sort": [{"published_at": {"order": "desc", "unmapped_type": "date"}}],
            "query": {"match_all": {}}
        }
        response = requests.get(f"{ES_URL}/{ES_INDEX}/_search", json=query, timeout=10)
        if response.status_code == 200:
            hits = response.json()['hits']['hits']
            data = [hit['_source'] for hit in hits]
            df = pd.DataFrame(data) if data else pd.DataFrame()
            
            # Rename and create missing columns for compatibility
            if not df.empty:
                # Use published_at as timestamp
                if 'published_at' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['published_at'], errors='coerce')
                elif 'event_time' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['event_time'], errors='coerce')
                
                # Use description as content if content doesn't exist
                if 'description' in df.columns and 'content' not in df.columns:
                    df['content'] = df['description']
                
                # Initialize missing columns with empty values
                if 'entities' not in df.columns:
                    df['entities'] = [[] for _ in range(len(df))]
                if 'locations' not in df.columns:
                    df['locations'] = [[] for _ in range(len(df))]
                if 'sentiment' not in df.columns:
                    df['sentiment'] = 'neutral'
                if 'sentiment_score' not in df.columns:
                    df['sentiment_score'] = 0.0
                if 'entity_count' not in df.columns:
                    df['entity_count'] = 0
                if 'theme' not in df.columns:
                    df['theme'] = 'general'
                    
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Elasticsearch error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_csv_data(file_path, size=2000):
    """Load hybrid dataset from CSV file"""
    try:
        df = pd.read_csv(file_path)
        # Convert timestamp to datetime without timezone (for compatibility)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True).dt.tz_localize(None)
        # Convert entities string back to list for compatibility
        df['entities'] = df['entities'].apply(
            lambda x: [e.strip() for e in str(x).split(', ')] if x and x != '' else []
        )
        # Convert locations string back to list
        df['locations'] = df['locations'].apply(
            lambda x: [l.strip() for l in str(x).split(', ')] if x and x != '' else []
        )
        # Add matched_event column if missing
        if 'matched_event' not in df.columns:
            df['matched_event'] = df['entity_count'].apply(lambda x: 1 if x >= 3 else 0)
        return df.head(size)
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return pd.DataFrame()

def enrich_elasticsearch_data(df, nlp):
    """Enrich Elasticsearch data with entities, locations, and sentiment"""
    if df.empty or nlp is None:
        return df
    
    try:
        # Extract entities and locations from content/description
        if 'content' in df.columns:
            for idx, row in df.iterrows():
                if pd.isna(row.get('content')):
                    continue
                    
                # Process text with spaCy
                doc = nlp(str(row['content'])[:1000])  # Limit to first 1000 chars for speed
                
                # Extract entities
                entities = [ent.text for ent in doc.ents if ent.label_ in ['PERSON', 'ORG', 'EVENT']]
                df.at[idx, 'entities'] = entities
                df.at[idx, 'entity_count'] = len(entities)
                
                # Extract locations
                locations = [ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']]
                df.at[idx, 'locations'] = locations
                
                # Simple sentiment analysis (can be improved)
                # For now, use keyword-based approach
                text_lower = str(row['content']).lower()
                positive_words = ['good', 'great', 'excellent', 'positive', 'success', 'win', 'best']
                negative_words = ['bad', 'terrible', 'negative', 'fail', 'worst', 'crisis', 'disaster']
                
                pos_count = sum(1 for word in positive_words if word in text_lower)
                neg_count = sum(1 for word in negative_words if word in text_lower)
                
                if pos_count > neg_count:
                    df.at[idx, 'sentiment'] = 'positive'
                    df.at[idx, 'sentiment_score'] = 0.6
                elif neg_count > pos_count:
                    df.at[idx, 'sentiment'] = 'negative'
                    df.at[idx, 'sentiment_score'] = -0.6
                else:
                    df.at[idx, 'sentiment'] = 'neutral'
                    df.at[idx, 'sentiment_score'] = 0.0
        
        return df
    except Exception as e:
        st.warning(f"Error enriching data: {e}")
        return df

# ============= ADVANCED ML FUNCTIONS =============

def generate_embeddings(texts, embedder):
    """Generate semantic embeddings using Sentence Transformers"""
    if not texts or embedder is None:
        return None
    try:
        embeddings = embedder.encode(texts, show_progress_bar=False)
        return embeddings
    except Exception as e:
        st.warning(f"Embedding generation error: {e}")
        return None

def cluster_with_hdbscan(embeddings, min_cluster_size=5):
    """Apply HDBSCAN clustering to embeddings"""
    if embeddings is None or len(embeddings) < min_cluster_size:
        return None
    try:
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, 
                                     min_samples=3,
                                     metric='euclidean')
        cluster_labels = clusterer.fit_predict(embeddings)
        return cluster_labels, clusterer.probabilities_
    except Exception as e:
        st.warning(f"HDBSCAN clustering error: {e}")
        return None, None

def generate_cluster_summary(cluster_texts, summarizer):
    """Generate AI summary for cluster using BART"""
    if not cluster_texts or summarizer is None or len(cluster_texts) < 2:
        return "Insufficient text for summarization"
    try:
        # Combine cluster texts (limit length for BART)
        combined_text = " ".join(cluster_texts[:10])  # First 10 texts
        if len(combined_text) < 50:
            return "Text too short for summarization"
        
        # Limit input length for BART
        max_input_length = 1024
        if len(combined_text) > max_input_length:
            combined_text = combined_text[:max_input_length]
        
        # Dynamically set max_length based on input
        input_length = len(combined_text.split())
        max_length = max(30, min(int(input_length * 0.7), 130))  # 70% of input or 130 max
        min_length = max(20, int(input_length * 0.3))  # 30% of input or 20 min
        
        summary = summarizer(combined_text, max_length=max_length, min_length=min_length, do_sample=False)
        return summary[0]['summary_text']
    except Exception as e:
        return f"Summarization error: {str(e)[:100]}"

def extract_locations_with_spacy(texts, nlp):
    """Extract geographic locations using spaCy NER"""
    if not texts or nlp is None:
        return []
    
    locations = []
    for text in texts[:100]:  # Limit for performance
        try:
            doc = nlp(str(text)[:500])  # Limit text length
            for ent in doc.ents:
                if ent.label_ in ['GPE', 'LOC']:  # Geopolitical Entity or Location
                    locations.append(ent.text)
        except:
            continue
    
    return Counter(locations).most_common(20)

def create_location_map(locations_with_counts):
    """Create folium map with location markers"""
    if not locations_with_counts:
        return None
    
    # Geocoding dictionary for common locations (expanded with India cities)
    location_coords = {
        'United States': (37.09, -95.71), 'USA': (37.09, -95.71), 'US': (37.09, -95.71), 'U.S.': (37.09, -95.71),
        'China': (35.86, 104.19), 'India': (20.59, 78.96),
        'Russia': (61.52, 105.31), 'Brazil': (-14.23, -51.92),
        'UK': (55.37, -3.43), 'United Kingdom': (55.37, -3.43), 'Britain': (55.37, -3.43),
        'France': (46.22, 2.21), 'Germany': (51.16, 10.45),
        'Japan': (36.20, 138.25), 'Australia': (-25.27, 133.77),
        'Canada': (56.13, -106.34), 'Mexico': (23.63, -102.55),
        'Italy': (41.87, 12.56), 'Spain': (40.46, -3.74),
        'South Korea': (35.90, 127.76), 'Argentina': (-38.41, -63.61),
        'Ukraine': (48.37, 31.16), 'Poland': (51.91, 19.14),
        'Turkey': (38.96, 35.24), 'Saudi Arabia': (23.88, 45.07),
        'Indonesia': (-0.78, 113.92), 'Netherlands': (52.13, 5.29),
        'Switzerland': (46.81, 8.22), 'Sweden': (60.12, 18.64),
        'Belgium': (50.50, 4.47), 'Austria': (47.51, 14.55),
        'Israel': (31.04, 34.85), 'Singapore': (1.35, 103.81),
        'Europe': (50.00, 10.00), 'South Africa': (-30.55, 22.93),
        'Pakistan': (30.37, 69.34), 'Bangladesh': (23.68, 90.35),
        'Syria': (34.80, 38.99), 'Iran': (32.42, 53.68),
        'Egypt': (26.82, 30.80), 'Nigeria': (9.08, 8.67),
        'New York': (40.71, -74.00), 'London': (51.50, -0.12),
        'Paris': (48.85, 2.35), 'Berlin': (52.52, 13.40),
        'Tokyo': (35.68, 139.69), 'Beijing': (39.90, 116.40),
        'Moscow': (55.75, 37.61), 'Washington': (38.89, -77.03),
        # India cities
        'Delhi': (28.70, 77.10), 'New Delhi': (28.70, 77.10),
        'Mumbai': (19.07, 72.87), 'Bombay': (19.07, 72.87),
        'Bangalore': (12.97, 77.59), 'Bengaluru': (12.97, 77.59),
        'Chennai': (13.08, 80.27), 'Madras': (13.08, 80.27),
        'Hyderabad': (17.38, 78.47),
        'Pune': (18.52, 73.85), 'Poona': (18.52, 73.85),
        'Kolkata': (22.57, 88.36), 'Calcutta': (22.57, 88.36),
        'Ahmedabad': (23.02, 72.57),
        'Jaipur': (26.91, 75.78),
        'Lucknow': (26.85, 80.95),
        'Surat': (21.17, 72.83),
        'Kanpur': (26.45, 80.33),
        'Nagpur': (21.14, 79.08),
        'Indore': (22.71, 75.85),
        'Thane': (19.21, 72.97),
        'Bhopal': (23.25, 77.41),
        'Visakhapatnam': (17.68, 83.21),
        'Patna': (25.59, 85.13),
        'Vadodara': (22.30, 73.18),
        'Ghaziabad': (28.66, 77.43),
        'Johannesburg': (-26.20, 28.04), 'Dubai': (25.20, 55.27),
        'Taiwan': (23.69, 120.96), 'Finland': (61.92, 25.74),
        'Sanna': (61.92, 25.74), 'Covid': (20.00, 0.00), 'A.I.': (37.09, -95.71)
    }
    
    # Create map centered on world
    m = folium.Map(location=[20, 0], zoom_start=2, tiles='CartoDB dark_matter')
    
    # Add markers for detected locations
    for loc, count in locations_with_counts:
        coords = location_coords.get(loc)
        if coords:
            folium.CircleMarker(
                location=coords,
                radius=min(count / 2, 20),
                popup=f"{loc}: {count} mentions",
                color='#667eea',
                fill=True,
                fillColor='#764ba2',
                fillOpacity=0.7
            ).add_to(m)
    
    return m

def analyze_temporal_lifecycle(df):
    """Advanced temporal analysis with lifecycle stages"""
    if df.empty or 'timestamp' not in df.columns:
        return None
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df_sorted = df.sort_values('timestamp')
    
    # Calculate entity count for each row if not present
    if 'entity_count' not in df_sorted.columns:
        df_sorted['entity_count'] = df_sorted['entities'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
    
    # Calculate metrics per hour
    agg_dict = {
        'content': 'count',
    }
    if 'sentiment_score' in df_sorted.columns:
        agg_dict['sentiment_score'] = 'mean'
    if 'entity_count' in df_sorted.columns:
        agg_dict['entity_count'] = 'sum'
    
    hourly_stats = df_sorted.set_index('timestamp').resample('1H').agg(agg_dict).reset_index()
    
    # Rename columns based on what was aggregated
    new_cols = ['timestamp', 'post_count']
    if 'sentiment_score' in agg_dict:
        new_cols.append('avg_sentiment')
    if 'entity_count' in agg_dict:
        new_cols.append('total_entities')
    hourly_stats.columns = new_cols
    
    # Calculate velocity (rate of change)
    hourly_stats['velocity'] = hourly_stats['post_count'].diff().fillna(0)
    hourly_stats['acceleration'] = hourly_stats['velocity'].diff().fillna(0)
    
    # Classify lifecycle stages
    def classify_stage(row):
        if row['velocity'] > 0 and row['acceleration'] > 0:
            return 'Birth/Growth'
        elif row['velocity'] > 0 and row['acceleration'] <= 0:
            return 'Maturity'
        elif row['velocity'] <= 0:
            return 'Decline'
        return 'Stable'
    
    hourly_stats['lifecycle_stage'] = hourly_stats.apply(classify_stage, axis=1)
    
    return hourly_stats

# ============= MAIN DASHBOARD =============

def main():
    # Real-time header with live indicator
    current_time = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
        <h1 style='text-align: center; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
        padding: 20px; border-radius: 10px; color: white;'>
        <span class='live-indicator'></span>🌐 Real-Time Narrative Intelligence Platform
        </h1>
        <p style='text-align: center; color: #94a3b8; font-size: 1.2em; margin-top: 10px;'>
        🚀 Live Streaming Analytics • Advanced ML: HDBSCAN • Transformers • BART • spaCy • Last Update: {current_time}
        </p>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Load ML models
    with st.spinner("🔮 Loading ML models..."):
        embedder, nlp, summarizer = load_ml_models()
        models_loaded = all([embedder is not None, nlp is not None, summarizer is not None])
        if models_loaded:
            st.success("✅ ML models loaded successfully")
        else:
            st.warning("⚠️ Some ML models failed to load. Basic features available.")
    
    # Real-time metrics at top
    metrics_row1 = st.columns(5)
    
    with metrics_row1[0]:
        total_docs = get_elasticsearch_stats()
        st.metric("📊 Total Documents", f"{total_docs:,}", help="Real-time document count")
    
    with metrics_row1[1]:
        # Calculate recent activity (last 5 minutes) - only for Elasticsearch
        if data_source == "Elasticsearch (Live)":
            try:
                five_min_ago = (datetime.now() - timedelta(minutes=5)).isoformat()
                recent_query = {
                    "query": {
                        "range": {
                            "timestamp": {
                                "gte": five_min_ago
                            }
                        }
                    }
                }
                recent_resp = requests.post(f"{ES_URL}/enriched_narratives/_count", json=recent_query, timeout=5)
                recent_count = recent_resp.json().get('count', 0) if recent_resp.status_code == 200 else 0
                delta = f"+{recent_count}" if recent_count > 0 else "0"
            except Exception as e:
                recent_count = 0
                delta = "0"
            st.metric("⚡ Last 5 Min", f"{recent_count}", delta=delta, help="Recent activity")
        else:
            # CSV mode - show data source info
            st.metric("📁 Data Source", "CSV", help="Hybrid dataset loaded")
    
    with metrics_row1[2]:
        df = get_elasticsearch_data(data_limit)
        positive = len(df[df['sentiment'] == 'positive']) if not df.empty and 'sentiment' in df.columns else 0
        total_sentiment = len(df[df['sentiment'].notna()]) if not df.empty and 'sentiment' in df.columns else 1
        pos_pct = f"{(positive/total_sentiment*100):.1f}%" if total_sentiment > 0 else "0%"
        st.metric("😊 Positive", f"{positive}", delta=pos_pct, help="Positive sentiment count")
    
    with metrics_row1[3]:
        negative = len(df[df['sentiment'] == 'negative']) if not df.empty and 'sentiment' in df.columns else 0
        neg_pct = f"{(negative/total_sentiment*100):.1f}%" if total_sentiment > 0 else "0%"
        st.metric("😠 Negative", f"{negative}", delta=neg_pct, help="Negative sentiment count")
    
    with metrics_row1[4]:
        locations_count = len(df[df['locations'].notna()]) if not df.empty and 'locations' in df.columns else 0
        loc_pct = f"{(locations_count/len(df)*100):.1f}%" if len(df) > 0 else "0%"
        st.metric("🌍 With Locations", f"{locations_count}", delta=loc_pct, help="Geotagged articles")
    
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("🎯 Advanced Features")
        
        st.markdown("""
        <div class='objective-box'>
        <b>🤖 HDBSCAN Clustering</b><br>
        Adaptive density-based event detection
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class='objective-box'>
        <b>🧠 Sentence Transformers</b><br>
        Semantic similarity embeddings
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class='objective-box'>
        <b>📝 BART Summarization</b><br>
        AI-generated event summaries
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class='objective-box'>
        <b>🗺️ Geospatial Analysis</b><br>
        Location extraction & mapping
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class='objective-box'>
        <b>⏱️ Lifecycle Tracking</b><br>
        Birth • Growth • Maturity • Decline
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.header("⚙️ Controls")
        auto_refresh = st.checkbox("Auto-refresh (10s)", value=False)
        sentiment_filter = st.multiselect(
            "Sentiment Filter",
            ["positive", "negative", "neutral"],
            default=["positive", "negative", "neutral"]
        )
        min_cluster_size = st.slider("Min Cluster Size (HDBSCAN)", 3, 20, 5)
        
        st.markdown("---")
        if st.button("🔄 Force Refresh"):
            st.cache_data.clear()
            st.rerun()
    
    # Load data based on source
    with st.spinner("Loading data..."):
        if data_source == "CSV File (Hybrid Dataset)":
            total_docs = 0  # CSV doesn't have total count
            df = load_csv_data(csv_path, size=data_limit)
            if not df.empty:
                total_docs = len(df)
        else:
            total_docs = get_elasticsearch_stats()
            df = get_elasticsearch_data(size=1000)  # Limit for ML processing
            
            # Enrich Elasticsearch data with entities and sentiment
            if not df.empty and nlp is not None:
                with st.spinner("Enriching data with ML models..."):
                    df = enrich_elasticsearch_data(df, nlp)
        
        # Remove duplicates based on title and timestamp
        if not df.empty and 'title' in df.columns:
            df = df.drop_duplicates(subset=['title'], keep='first')
        
        # Add matched_event column if missing (for Events metric)
        if not df.empty and 'matched_event' not in df.columns:
            # Simple event detection: articles with multiple entities are likely events
            if 'entities' in df.columns:
                df['matched_event'] = df['entities'].apply(
                    lambda x: 1 if isinstance(x, list) and len(x) >= 3 else 0
                )
            else:
                df['matched_event'] = 0
    
    # Apply filters
    if not df.empty and 'sentiment' in df.columns:
        if sentiment_filter:
            df = df[df['sentiment'].isin(sentiment_filter)]
    
    st.markdown("---")
    
    # Real-Time Activity Dashboard
    st.markdown("### 📈 Real-Time Activity Dashboard")
    activity_col1, activity_col2 = st.columns([2, 1])
    
    with activity_col1:
        if not df.empty and 'timestamp' in df.columns:
            # Activity over last hour
            df_temp = df.copy()
            df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'], errors='coerce')
            # Make datetime comparison timezone-aware for compatibility
            cutoff_time = pd.Timestamp(datetime.now() - timedelta(hours=1), tz='UTC')
            df_recent = df_temp[df_temp['timestamp'] > cutoff_time]
            
            if not df_recent.empty:
                df_recent['minute'] = df_recent['timestamp'].dt.floor('min')
                activity_chart = df_recent.groupby('minute').size().reset_index(name='count')
                
                fig_activity = px.line(
                    activity_chart,
                    x='minute',
                    y='count',
                    title='📊 Activity in Last Hour (per minute)',
                    labels={'minute': 'Time', 'count': 'Documents'}
                )
                fig_activity.update_traces(line_color='#3b82f6', line_width=3)
                fig_activity.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f1f5f9'),
                    height=250
                )
                st.plotly_chart(fig_activity, use_container_width=True)
    
    with activity_col2:
        if not df.empty and 'locations' in df.columns:
            # Trending locations
            st.markdown("#### 🔥 Trending Now")
            all_locs = []
            for loc_list in df['locations'].dropna():
                if isinstance(loc_list, list):
                    all_locs.extend(loc_list)
            
            if all_locs:
                trending = Counter(all_locs).most_common(5)
                for i, (loc, count) in enumerate(trending, 1):
                    st.markdown(f"**{i}.** {loc} `{count}` 📍")
    
    st.markdown("---")
    
    # Tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🤖 HDBSCAN Clustering",
        "📝 AI Summaries",
        "🗺️ Geospatial Map",
        "⏱️ Lifecycle Analysis",
        "🌐 Entity Network",
        "📱 Live Feed"
    ])
    
    # TAB 1: HDBSCAN Clustering
    with tab1:
        st.subheader("🤖 HDBSCAN Adaptive Clustering")
        
        if not df.empty and 'content' in df.columns and embedder is not None:
            with st.spinner("Generating semantic embeddings..."):
                texts = df['content'].dropna().astype(str).tolist()[:500]  # Limit for performance
                embeddings = generate_embeddings(texts, embedder)
            
            if embeddings is not None:
                # Narrative Drift Detection
                st.markdown("### 🧬 Narrative Drift Analysis")
                st.markdown("*Detecting when story meanings shift over time*")
                
                # Check if we have historical embeddings (using session state)
                if 'prev_embeddings' in st.session_state:
                    drift_score, old_centroid, new_centroid = calculate_narrative_drift(
                        st.session_state['prev_embeddings'], embeddings
                    )
                    
                    drift_cols = st.columns(3)
                    drift_cols[0].metric("📊 Drift Score", f"{drift_score:.3f}", 
                                        help="0 = No drift, 1 = Complete shift")
                    
                    if drift_score > 0.3:
                        drift_cols[1].markdown("🚨 **HIGH DRIFT DETECTED**")
                        drift_cols[2].markdown("*Story meaning has shifted significantly*")
                    elif drift_score > 0.15:
                        drift_cols[1].markdown("⚠️ **Moderate Drift**")
                        drift_cols[2].markdown("*Story is evolving*")
                    else:
                        drift_cols[1].markdown("✅ **Stable Narrative**")
                        drift_cols[2].markdown("*Story remains consistent*")
                else:
                    st.info("📊 Establishing baseline... Drift analysis will appear on next refresh")
                
                # Store current embeddings for next comparison
                st.session_state['prev_embeddings'] = embeddings[:100]  # Store sample
                
                st.markdown("---")
                
                with st.spinner("Running HDBSCAN clustering..."):
                    cluster_labels, probabilities = cluster_with_hdbscan(embeddings, min_cluster_size)
                
                if cluster_labels is not None:
                    # Create clustered dataframe with indices to preserve relationship to original df
                    df_clustered = df.iloc[:len(texts)].copy()
                    df_clustered['cluster'] = cluster_labels
                    df_clustered['probability'] = probabilities if probabilities is not None else [0] * len(cluster_labels)
                    df_clustered['text'] = texts
                    
                    # Cluster statistics
                    n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
                    n_noise = list(cluster_labels).count(-1)
                    
                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("🎯 Clusters Found", n_clusters)
                    col_b.metric("📊 Clustered Posts", len(cluster_labels) - n_noise)
                    col_c.metric("🔇 Noise Points", n_noise)
                    
                    # Cluster size distribution
                    cluster_sizes = df_clustered[df_clustered['cluster'] != -1].groupby('cluster').size().reset_index(name='size')
                    cluster_sizes = cluster_sizes.sort_values('size', ascending=False)
                    
                    fig_clusters = px.bar(
                        cluster_sizes.head(15),
                        x='cluster',
                        y='size',
                        title='Cluster Size Distribution (Top 15)',
                        labels={'cluster': 'Cluster ID', 'size': 'Number of Posts'},
                        color='size',
                        color_continuous_scale='Viridis'
                    )
                    fig_clusters.update_layout(
                        template='plotly_dark',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#f1f5f9')
                    )
                    st.plotly_chart(fig_clusters, use_container_width=True)
                    
                    # Show sample posts from each cluster with AI summaries
                    st.markdown("### 📋 Intelligent Cluster Analysis")
                    
                    for cluster_id in cluster_sizes.head(5)['cluster']:
                        cluster_df = df_clustered[df_clustered['cluster'] == cluster_id]
                        cluster_posts = cluster_df['text'].tolist()
                        
                        with st.expander(f"🎯 Cluster {cluster_id} - {len(cluster_posts)} articles", expanded=False):
                            # Generate AI summary for this cluster
                            if summarizer is not None and len(cluster_posts) >= 2:
                                col_summary, col_samples = st.columns([2, 1])
                                
                                with col_summary:
                                    st.markdown("#### 🤖 AI Cluster Summary")
                                    with st.spinner(f"Analyzing cluster {cluster_id}..."):
                                        cluster_summary = generate_cluster_summary(cluster_posts[:10], summarizer)
                                    
                                    st.markdown(f"""
                                    <div class='summary-box'>
                                    {cluster_summary}
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Extract key entities from this cluster
                                    cluster_entities = []
                                    for entities in cluster_df['entities'].dropna():
                                        if isinstance(entities, list):
                                            cluster_entities.extend(entities)
                                    
                                    if cluster_entities:
                                        top_cluster_entities = Counter(cluster_entities).most_common(5)
                                        st.markdown("**🏷️ Key Entities:**")
                                        entity_badges = " ".join([f"`{ent}` ({count})" for ent, count in top_cluster_entities])
                                        st.markdown(entity_badges)
                                
                                with col_samples:
                                    st.markdown("#### 📊 Cluster Stats")
                                    
                                    # Sentiment distribution in cluster
                                    cluster_sentiments = cluster_df['sentiment'].value_counts()
                                    if not cluster_sentiments.empty:
                                        st.markdown("**Sentiment:**")
                                        for sent, count in cluster_sentiments.items():
                                            emoji = "😊" if sent == "positive" else "😠" if sent == "negative" else "😐"
                                            st.write(f"{emoji} {sent.title()}: {count}")
                                    
                                    # Location distribution
                                    cluster_locs = []
                                    for locs in cluster_df['locations'].dropna():
                                        if isinstance(locs, list):
                                            cluster_locs.extend(locs)
                                    if cluster_locs:
                                        top_locs = Counter(cluster_locs).most_common(3)
                                        st.markdown("**🌍 Top Locations:**")
                                        for loc, count in top_locs:
                                            st.write(f"📍 {loc}: {count}")
                                
                                st.markdown("---")
                                st.markdown("**📄 Sample Articles:**")
                                for i, (idx, row) in enumerate(cluster_df.head(3).iterrows()):
                                    title = row.get('title', 'No title')
                                    content_preview = row.get('text', row.get('content', ''))[:150]
                                    st.write(f"**{i+1}. {title}**")
                                    st.write(f"   {content_preview}...")
                            else:
                                # Fallback if no summarizer
                                st.markdown("**📄 Sample Articles:**")
                                for i, (idx, row) in enumerate(cluster_df.head(3).iterrows()):
                                    title = row.get('title', 'No title')
                                    content_preview = row.get('text', row.get('content', ''))[:150]
                                    st.write(f"**{i+1}. {title}**")
                                    st.write(f"   {content_preview}...")
                else:
                    st.warning("HDBSCAN clustering failed or insufficient data")
            else:
                st.warning("Embedding generation failed")
        else:
            st.info("Insufficient data or models not loaded")
    
    # TAB 2: AI Summarization (Enhanced)
    with tab2:
        st.subheader("🧠 AI-Powered Narrative Intelligence")
        
        if not df.empty and 'content' in df.columns and summarizer is not None:
            # Section 1: Top Stories by Entity
            st.markdown("### 🎯 Top Story Topics")
            
            # Extract top entities
            all_entities = []
            for entities in df['entities'].dropna():
                if isinstance(entities, list):
                    all_entities.extend(entities)
            
            if all_entities:
                top_entities = Counter(all_entities).most_common(5)
                
                entity_cols = st.columns(min(3, len(top_entities)))
                for idx, (entity, count) in enumerate(top_entities[:3]):
                    with entity_cols[idx]:
                        # Get articles mentioning this entity
                        entity_articles = df[df['entities'].apply(
                            lambda x: entity in x if isinstance(x, list) else False
                        )]
                        
                        if len(entity_articles) >= 2:
                            texts = entity_articles['content'].dropna().astype(str).tolist()[:10]
                            with st.spinner(f"Analyzing {entity}..."):
                                topic_summary = generate_cluster_summary(texts, summarizer)
                            
                            st.markdown(f"""
                            <div class='objective-box'>
                            <h4>📌 {entity}</h4>
                            <p style='font-size: 0.9em;'><strong>{count} mentions</strong></p>
                            <p style='font-size: 0.85em; margin-top: 10px;'>{topic_summary}</p>
                            </div>
                            """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Section 2: Timeline Analysis
            st.markdown("### 📅 Timeline Intelligence")
            
            if 'timestamp' in df.columns:
                df_temp = df.copy()
                df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'], errors='coerce')
                
                time_cols = st.columns(2)
                
                with time_cols[0]:
                    st.markdown("**🕐 Last Hour**")
                    recent_1h = df_temp[df_temp['timestamp'] > pd.Timestamp(datetime.now() - timedelta(hours=1), tz='UTC')]
                    if len(recent_1h) >= 2:
                        recent_texts = recent_1h['content'].dropna().astype(str).tolist()[:15]
                        with st.spinner("Analyzing recent activity..."):
                            recent_summary = generate_cluster_summary(recent_texts, summarizer)
                        st.info(f"**{len(recent_1h)} articles** - {recent_summary}")
                    else:
                        st.warning("No recent activity")
                
                with time_cols[1]:
                    st.markdown("**📆 Last 24 Hours**")
                    recent_24h = df_temp[df_temp['timestamp'] > pd.Timestamp(datetime.now() - timedelta(hours=24), tz='UTC')]
                    if len(recent_24h) >= 2:
                        day_texts = recent_24h['content'].dropna().astype(str).tolist()[:20]
                        with st.spinner("Analyzing daily trends..."):
                            day_summary = generate_cluster_summary(day_texts, summarizer)
                        st.info(f"**{len(recent_24h)} articles** - {day_summary}")
                    else:
                        st.warning("Limited data")
            
            st.markdown("---")
            
            # Section 3: Critical Alerts (Negative sentiment)
            st.markdown("### ⚠️ Critical Intelligence Alerts")
            
            negative_articles = df[df['sentiment'] == 'negative']
            if len(negative_articles) >= 2:
                # Get most recent negative articles
                if 'timestamp' in negative_articles.columns:
                    negative_articles = negative_articles.sort_values('timestamp', ascending=False)
                
                critical_texts = negative_articles['content'].dropna().astype(str).tolist()[:10]
                with st.spinner("Analyzing critical narratives..."):
                    critical_summary = generate_cluster_summary(critical_texts, summarizer)
                
                st.error(f"""
                **🚨 Alert Summary ({len(negative_articles)} negative articles)**  
                {critical_summary}
                """)
                
                # Show top 3 critical headlines
                if 'title' in negative_articles.columns:
                    st.markdown("**Top Critical Headlines:**")
                    for idx, row in negative_articles.head(3).iterrows():
                        title = row.get('title', 'No title')
                        location = row.get('locations', ['Unknown'])[0] if isinstance(row.get('locations'), list) and row.get('locations') else 'Unknown'
                        st.markdown(f"- 📍 **{location}**: {title}")
            else:
                st.success("✅ No critical alerts detected")
            
            st.markdown("---")
            
            # Section 4: Positive Developments
            st.markdown("### 📈 Positive Developments")
            
            positive_articles = df[df['sentiment'] == 'positive']
            if len(positive_articles) >= 2:
                positive_texts = positive_articles['content'].dropna().astype(str).tolist()[:10]
                with st.spinner("Analyzing positive trends..."):
                    positive_summary = generate_cluster_summary(positive_texts, summarizer)
                
                st.success(f"""
                **😊 Positive Trends ({len(positive_articles)} articles)**  
                {positive_summary}
                """)
            else:
                st.info("No significant positive trends")
            
            st.markdown("---")
            
            # Section 5: Hype vs. Substance Analysis (Clickbait Detection)
            st.markdown("### ⚡ Hype vs. Substance Index")
            st.markdown("*Detecting clickbait: High sensationalism + Low factual density*")
            
            if 'entities' in df.columns and 'title' in df.columns:
                hype_data = []
                for idx, row in df.head(100).iterrows():
                    title = row.get('title', '')
                    content = row.get('content', '')
                    entities = row.get('entities', [])
                    
                    hype_score, sensationalism, entity_density = calculate_hype_score(
                        title + ' ' + str(content)[:200], entities
                    )
                    
                    hype_data.append({
                        'title': title[:60] + '...' if len(title) > 60 else title,
                        'hype_score': hype_score,
                        'sensationalism': sensationalism,
                        'factual_density': entity_density,
                        'sentiment': row.get('sentiment', 'neutral')
                    })
                
                if hype_data:
                    hype_df = pd.DataFrame(hype_data)
                    
                    # Scatter plot: Factual Depth vs Sensationalism
                    fig_hype = px.scatter(
                        hype_df,
                        x='factual_density',
                        y='sensationalism',
                        size='hype_score',
                        color='hype_score',
                        hover_data=['title'],
                        title='📊 Hype vs. Substance Map',
                        labels={
                            'factual_density': 'Factual Density (Entities per Word)',
                            'sensationalism': 'Sensationalism Score',
                            'hype_score': 'Clickbait Risk'
                        },
                        color_continuous_scale='Reds'
                    )
                    
                    # Add quadrant lines
                    fig_hype.add_hline(y=5, line_dash="dash", line_color="gray", opacity=0.5)
                    fig_hype.add_vline(x=2, line_dash="dash", line_color="gray", opacity=0.5)
                    
                    fig_hype.update_layout(
                        template='plotly_dark',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#f1f5f9'),
                        height=450
                    )
                    st.plotly_chart(fig_hype, use_container_width=True)
                    
                    # Show top clickbait articles
                    top_hype = hype_df.nlargest(5, 'hype_score')
                    if len(top_hype) > 0:
                        st.markdown("**🚨 Potential Clickbait Articles:**")
                        for idx, row in top_hype.iterrows():
                            if row['hype_score'] > 5:
                                st.warning(f"⚠️ **Hype Score: {row['hype_score']:.1f}** - {row['title']}")
                
        else:
            st.info("Summarizer not loaded or insufficient data")
    
    # TAB 3: Geospatial Analysis
    with tab3:
        st.subheader("🗺️ Geographic Event Distribution")
        
        if not df.empty:
            # Try to use pre-extracted locations first, fall back to spaCy extraction
            locations = []
            
            if 'locations' in df.columns:
                # Use pre-extracted locations from Elasticsearch
                with st.spinner("Loading geographic data..."):
                    all_locs = []
                    for loc_list in df['locations'].dropna():
                        if isinstance(loc_list, list):
                            all_locs.extend(loc_list)
                        elif isinstance(loc_list, str) and loc_list:
                            all_locs.append(loc_list)
                    locations = Counter(all_locs).most_common(20)
            
            # Fall back to spaCy extraction if no pre-extracted locations
            if not locations and 'content' in df.columns and nlp is not None:
                with st.spinner("Extracting locations with spaCy NER..."):
                    texts = df['content'].dropna().astype(str).tolist()
                    locations = extract_locations_with_spacy(texts, nlp)
            
            if locations:
                # Add world heat map visualization
                st.markdown("### 🌐 Global Coverage Heatmap")
                
                # Create country-level aggregation for choropleth
                country_map = {
                    'US': 'United States', 'U.S.': 'United States', 'USA': 'United States',
                    'UK': 'United Kingdom', 'Russia': 'Russia', 'China': 'China',
                    'India': 'India', 'Japan': 'Japan', 'Germany': 'Germany',
                    'France': 'France', 'Brazil': 'Brazil', 'Canada': 'Canada',
                    'Australia': 'Australia', 'Ukraine': 'Ukraine', 'South Africa': 'South Africa'
                }
                
                country_counts = {}
                for loc, count in locations:
                    country = country_map.get(loc, loc)
                    country_counts[country] = country_counts.get(country, 0) + count
                
                if country_counts:
                    country_df = pd.DataFrame(list(country_counts.items()), columns=['Country', 'Mentions'])
                    
                    fig_world = px.choropleth(
                        country_df,
                        locations='Country',
                        locationmode='country names',
                        color='Mentions',
                        title='📍 Global Narrative Coverage',
                        color_continuous_scale='Viridis',
                        hover_data={'Mentions': True}
                    )
                    fig_world.update_layout(
                        template='plotly_dark',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#f1f5f9'),
                        height=400
                    )
                    st.plotly_chart(fig_world, use_container_width=True)
                
                # Show table first (collapsible)
                with st.expander("📍 Top Locations by Mentions", expanded=False):
                    loc_df = pd.DataFrame(locations, columns=['Location', 'Mentions'])
                    st.dataframe(loc_df, use_container_width=True, hide_index=True)
                
                # Map below (full width, not blocked)
                st.markdown("### 🗺️ Interactive Point Map")
                location_map = create_location_map(locations)
                if location_map:
                    st_folium(location_map, width=1200, height=600, returned_objects=[])
                else:
                    st.info("No mappable locations found")
            else:
                st.info("No locations detected in current data")
        else:
            st.info("Insufficient data for geospatial analysis")
    
    # TAB 4: Temporal Lifecycle
    with tab4:
        st.subheader("⏱️ Event Lifecycle Analysis")
        
        if not df.empty:
            lifecycle_data = analyze_temporal_lifecycle(df)
            
            if lifecycle_data is not None and not lifecycle_data.empty:
                # Post count over time
                fig_posts = px.area(
                    lifecycle_data,
                    x='timestamp',
                    y='post_count',
                    title='Post Volume Over Time',
                    labels={'timestamp': 'Time', 'post_count': 'Posts/Hour'}
                )
                fig_posts.update_traces(fillcolor='rgba(102, 126, 234, 0.3)', line_color='#667eea')
                fig_posts.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f1f5f9')
                )
                st.plotly_chart(fig_posts, use_container_width=True)
                
                # Velocity and acceleration
                fig_velocity = go.Figure()
                fig_velocity.add_trace(go.Scatter(
                    x=lifecycle_data['timestamp'],
                    y=lifecycle_data['velocity'],
                    mode='lines',
                    name='Velocity (Growth Rate)',
                    line=dict(color='#10b981', width=2)
                ))
                fig_velocity.add_trace(go.Scatter(
                    x=lifecycle_data['timestamp'],
                    y=lifecycle_data['acceleration'],
                    mode='lines',
                    name='Acceleration (Momentum)',
                    line=dict(color='#ef4444', width=2)
                ))
                fig_velocity.update_layout(
                    title='Event Velocity & Acceleration',
                    template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f1f5f9'),
                    yaxis_title='Rate of Change'
                )
                st.plotly_chart(fig_velocity, use_container_width=True)
                
                # Lifecycle stage distribution
                stage_counts = lifecycle_data['lifecycle_stage'].value_counts()
                fig_stages = px.pie(
                    values=stage_counts.values,
                    names=stage_counts.index,
                    title='Lifecycle Stage Distribution',
                    color_discrete_sequence=['#10b981', '#3b82f6', '#ef4444', '#6b7280']
                )
                fig_stages.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f1f5f9')
                )
                st.plotly_chart(fig_stages, use_container_width=True)
                
                # Detailed lifecycle table
                st.markdown("### 📊 Lifecycle Metrics")
                display_cols = ['timestamp', 'post_count', 'velocity', 'acceleration', 'lifecycle_stage']
                st.dataframe(lifecycle_data[display_cols].tail(24), use_container_width=True)
            else:
                st.info("Collecting temporal data...")
        else:
            st.info("No data available")
    
    # TAB 5: Dynamic Entity Relationship Network
    with tab5:
        st.subheader("🕸️ Dynamic Entity Relationship Graph")
        st.markdown("*Real-time sentiment between entities - Who is connected to whom?*")
        
        if not df.empty and 'entities' in df.columns and nlp is not None:
            # Extract entity relationships with sentiment
            with st.spinner("Building entity relationship network..."):
                relationships = extract_entity_relationships(df.head(200), nlp)
            
            if relationships:
                # Create network graph
                edge_data, nodes = create_entity_network_graph(relationships)
                
                # Show metrics
                metric_cols = st.columns(3)
                metric_cols[0].metric("🔗 Total Connections", len(edge_data))
                metric_cols[1].metric("👥 Unique Entities", len(nodes))
                metric_cols[2].metric("📊 Analyzed Articles", min(200, len(df)))
                
                st.markdown("---")
                
                # Relationship strength analysis
                st.markdown("### 📊 Top Entity Relationships")
                
                # Convert to dataframe for display
                rel_df = pd.DataFrame(edge_data)
                if not rel_df.empty:
                    rel_df = rel_df.sort_values('weight', ascending=False).head(20)
                    
                    # Create network visualization using plotly
                    import networkx as nx
                    G = nx.Graph()
                    
                    # Add edges
                    for idx, row in rel_df.iterrows():
                        G.add_edge(row['source'], row['target'], 
                                  weight=row['weight'], 
                                  sentiment=row['sentiment'])
                    
                    # Get positions using spring layout
                    pos = nx.spring_layout(G, k=0.5, iterations=50)
                    
                    # Create plotly figure
                    edge_traces = []
                    for edge in G.edges(data=True):
                        x0, y0 = pos[edge[0]]
                        x1, y1 = pos[edge[1]]
                        
                        sentiment = edge[2]['sentiment']
                        # Color based on sentiment
                        if sentiment > 0.2:
                            color = 'rgba(0, 255, 0, 0.5)'
                        elif sentiment < -0.2:
                            color = 'rgba(255, 0, 0, 0.5)'
                        else:
                            color = 'rgba(128, 128, 128, 0.3)'
                        
                        edge_trace = go.Scatter(
                            x=[x0, x1, None],
                            y=[y0, y1, None],
                            mode='lines',
                            line=dict(width=edge[2]['weight'] * 0.5, color=color),
                            hoverinfo='none',
                            showlegend=False
                        )
                        edge_traces.append(edge_trace)
                    
                    # Create node trace
                    node_x = []
                    node_y = []
                    node_text = []
                    node_size = []
                    
                    for node in G.nodes():
                        x, y = pos[node]
                        node_x.append(x)
                        node_y.append(y)
                        node_text.append(node)
                        node_size.append(G.degree(node) * 5 + 10)
                    
                    node_trace = go.Scatter(
                        x=node_x,
                        y=node_y,
                        mode='markers+text',
                        text=node_text,
                        textposition="top center",
                        marker=dict(
                            size=node_size,
                            color='#667eea',
                            line=dict(color='white', width=2)
                        ),
                        textfont=dict(size=10, color='white'),
                        hoverinfo='text',
                        showlegend=False
                    )
                    
                    # Create figure
                    fig_network = go.Figure(data=edge_traces + [node_trace])
                    fig_network.update_layout(
                        title='🕸️ Entity Relationship Network (Edge color: Green=Positive, Red=Negative, Gray=Neutral)',
                        showlegend=False,
                        hovermode='closest',
                        template='plotly_dark',
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)',
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        height=600
                    )
                    
                    st.plotly_chart(fig_network, use_container_width=True)
                    
                    # Show relationship table
                    st.markdown("### 📋 Relationship Details")
                    
                    display_df = rel_df[['source', 'target', 'weight', 'sentiment']].copy()
                    display_df['sentiment_label'] = display_df['sentiment'].apply(
                        lambda x: '😊 Positive' if x > 0.2 else '😠 Negative' if x < -0.2 else '😐 Neutral'
                    )
                    display_df['sentiment'] = display_df['sentiment'].round(2)
                    display_df.columns = ['Entity 1', 'Entity 2', 'Co-occurrences', 'Avg Sentiment', 'Relationship']
                    
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
            else:
                st.info("Insufficient entity pairs for network analysis")
        else:
            st.info("No entity data available or NLP model not loaded")
    
    # TAB 6: Live Feed (from original)
    with tab6:
        st.subheader("📱 Real-Time Stream")
        
        if not df.empty:
            recent_posts = df.head(20)
            
            for idx, row in recent_posts.iterrows():
                sentiment_emoji = "😊" if row.get('sentiment') == 'positive' else "😠" if row.get('sentiment') == 'negative' else "😐"
                event_badge = "🎯" if row.get('matched_event') else ""
                
                # Safe content extraction
                content = row.get('content') or row.get('text') or row.get('title') or 'No content'
                content_preview = str(content)[:80] if content else 'No content'
                
                with st.expander(f"{sentiment_emoji} {event_badge} {content_preview}..."):
                    st.write(f"**Content:** {content}")
                    st.write(f"**Sentiment:** {row.get('sentiment', 'neutral')} ({row.get('sentiment_score', 0):.2f})")
                    
                    entities = row.get('entities', [])
                    if isinstance(entities, list) and entities:
                        st.write(f"**Entities:** {', '.join(str(e) for e in entities[:5])}")
        else:
            st.warning("No live data available")
    
    # Critical Alerts Sidebar
    with st.sidebar:
        st.markdown("---")
        st.markdown("## 🚨 Critical Alerts")
        
        # Alert for high negative sentiment surge
        if not df.empty and 'sentiment' in df.columns:
            negative_count = len(df[df['sentiment'] == 'negative'])
            total = len(df)
            negative_ratio = negative_count / total if total > 0 else 0
            
            if negative_ratio > 0.5:
                st.error(f"⚠️ **High Negativity**: {negative_ratio*100:.0f}% negative articles")
            
            # Alert for narrative drift
            if 'prev_embeddings' in st.session_state:
                drift_score = getattr(st.session_state, 'last_drift_score', 0)
                if drift_score > 0.3:
                    st.warning(f"🧬 **Narrative Shift**: Drift score {drift_score:.2f}")
            
            # Alert for high hype articles
            critical_entities = ['war', 'crisis', 'disaster', 'attack', 'threat']
            critical_count = 0
            if 'entities' in df.columns:
                for entities in df['entities'].dropna():
                    if isinstance(entities, list):
                        if any(e.lower() in critical_entities for e in entities):
                            critical_count += 1
            
            if critical_count > 5:
                st.error(f"🔴 **Critical Events**: {critical_count} crisis-related articles")
        
        st.markdown("---")
        st.markdown("## 📤 Export Data")
        
        # Export button
        if st.button("💾 Export Analysis Report"):
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'total_documents': len(df),
                'sentiment_distribution': df['sentiment'].value_counts().to_dict() if 'sentiment' in df.columns else {},
                'top_entities': dict(Counter([e for entities in df['entities'].dropna() 
                                             for e in (entities if isinstance(entities, list) else [])]).most_common(10)),
                'system_status': {
                    'ml_models_active': models_loaded,
                    'data_quality': 'Good' if len(df) > 100 else 'Limited'
                }
            }
            
            st.download_button(
                label="📥 Download JSON Report",
                data=json.dumps(export_data, indent=2),
                file_name=f"narrative_intel_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
            st.success("✅ Report ready for download!")
    
    # Footer with system status
    st.markdown("---")
    status_cols = st.columns([1, 1, 1, 1])
    with status_cols[0]:
        st.markdown(f"**System:** 🟢 Live Streaming")
    with status_cols[1]:
        st.markdown(f"**ML Models:** {'✅ Active' if models_loaded else '⚠️ Partial'}")
    with status_cols[2]:
        st.markdown(f"**Last Update:** {datetime.now().strftime('%H:%M:%S')}")
    with status_cols[3]:
        st.markdown(f"**Next Refresh:** {refresh_interval}s")
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
