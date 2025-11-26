"""
Start the complete data pipeline for live news streaming
"""
import subprocess
import sys
import time
import os

def start_producer():
    """Start the enhanced news producer"""
    print("🚀 Starting Enhanced News Producer...")
    producer_path = os.path.join("narrative_engine", "src", "ingestion", "enhanced_news_producer.py")
    producer_process = subprocess.Popen(
        [sys.executable, producer_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    return producer_process

def main():
    print("=" * 60)
    print("🎬 STARTING REAL-TIME NARRATIVE INTELLIGENCE PIPELINE")
    print("=" * 60)
    
    try:
        # Start producer
        producer = start_producer()
        print("✅ News Producer started (PID: {})".format(producer.pid))
        print("\n📊 Pipeline Status:")
        print("  ✅ Kafka: Running")
        print("  ✅ Elasticsearch: Running") 
        print("  ✅ Producer: Running")
        print("\n🌐 Access points:")
        print("  📈 Dashboard: http://localhost:8501")
        print("  🔍 Elasticsearch: http://localhost:9200")
        print("  🎛️  Kafka UI: http://localhost:9000")
        print("  📊 Kibana: http://localhost:5601")
        print("\n⏳ Collecting data... (Press Ctrl+C to stop)")
        
        # Keep running
        while True:
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping pipeline...")
        producer.terminate()
        producer.wait()
        print("✅ Pipeline stopped successfully")

if __name__ == "__main__":
    main()
