"""
Kafka Consumer for Elasticsearch Indexing
Consumes news from Kafka and indexes into Elasticsearch with enrichment
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
import spacy

# Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'raw_news'
ES_URL = 'http://localhost:9200'
ES_INDEX = 'news_articles'

print("🔧 Initializing Elasticsearch consumer...")

# Initialize Elasticsearch
es = Elasticsearch([ES_URL])

# Test connection
if es.ping():
    print("✅ Connected to Elasticsearch")
    # Create index if it doesn't exist
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX)
        print(f"✅ Created index: {ES_INDEX}")
else:
    print("❌ Failed to connect to Elasticsearch")
    exit(1)

# Initialize spaCy for entity extraction
print("🔧 Loading spaCy model...")
try:
    nlp = spacy.load('en_core_web_sm')
    print("✅ spaCy model loaded")
except:
    print("⚠️ spaCy model not found. Install with: python -m spacy download en_core_web_sm")
    nlp = None

# Initialize Kafka Consumer
print("🔧 Connecting to Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='elasticsearch-indexer'
)

print(f"✅ Kafka consumer connected. Listening to topic: {KAFKA_TOPIC}")
print("🔄 Starting to consume messages...\n")

def extract_entities(text):
    """Extract entities using spaCy"""
    if not text or nlp is None:
        return [], []
    
    try:
        doc = nlp(text[:1000])  # Limit to first 1000 chars
        entities = [ent.text for ent in doc.ents if ent.label_ in ['PERSON', 'ORG', 'EVENT']]
        locations = [ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']]
        return entities, locations
    except:
        return [], []

def analyze_sentiment(text):
    """Simple keyword-based sentiment analysis"""
    if not text:
        return 'neutral', 0.0
    
    text_lower = text.lower()
    positive_words = ['good', 'great', 'excellent', 'positive', 'success', 'win', 'best', 'amazing', 'wonderful']
    negative_words = ['bad', 'terrible', 'negative', 'fail', 'worst', 'crisis', 'disaster', 'poor', 'awful']
    
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    if pos_count > neg_count:
        return 'positive', 0.6
    elif neg_count > pos_count:
        return 'negative', -0.6
    else:
        return 'neutral', 0.0

# Process messages
message_count = 0
batch = []
batch_size = 10

try:
    for message in consumer:
        try:
            article = message.value
            
            # Enrich article with entities and sentiment
            content = article.get('description', '') or article.get('content', '')
            entities, locations = extract_entities(content)
            sentiment, sentiment_score = analyze_sentiment(content)
            
            # Prepare document for Elasticsearch
            doc = {
                'id': article.get('id', f"{article.get('title', '')[:20]}_{int(time.time())}"),
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'content': content,
                'source': article.get('source', {}).get('name', 'Unknown') if isinstance(article.get('source'), dict) else article.get('source', 'Unknown'),
                'author': article.get('author', 'Unknown'),
                'url': article.get('url', ''),
                'published_at': article.get('publishedAt', article.get('published_at', datetime.now().isoformat())),
                'ingested_at': datetime.now().isoformat(),
                'timestamp': article.get('publishedAt', article.get('published_at', datetime.now().isoformat())),
                'event_time': datetime.now().isoformat(),
                'entities': entities,
                'entity_count': len(entities),
                'locations': locations,
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'theme': 'live_news'
            }
            
            batch.append({
                '_index': ES_INDEX,
                '_id': doc['id'],
                '_source': doc
            })
            
            message_count += 1
            
            # Bulk index when batch is full
            if len(batch) >= batch_size:
                success, failed = helpers.bulk(es, batch, raise_on_error=False)
                print(f"✅ Indexed batch: {success} docs | Total: {message_count} | Failed: {len(failed)}")
                batch = []
            
            # Show progress
            if message_count % 5 == 0:
                print(f"📊 Processed {message_count} articles from Kafka")
                
        except Exception as e:
            print(f"❌ Error processing message: {e}")
            continue
            
except KeyboardInterrupt:
    print("\n⏹️ Stopping consumer...")
    
    # Index remaining documents
    if batch:
        success, failed = helpers.bulk(es, batch, raise_on_error=False)
        print(f"✅ Final batch indexed: {success} docs | Failed: {len(failed)}")
    
    print(f"\n📊 Total messages processed: {message_count}")
    consumer.close()
    print("✅ Consumer closed")
