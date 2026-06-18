import json
import time
import os
import requests
from datetime import datetime, UTC
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Kafka configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'stock-prices'

# Stock and their partition assignments
STOCKS = {
    'RELIANCE.BSE': 0,
    'TCS.BSE': 1,
    'HDFCBANK.BSE': 2,
    'INFY.BSE': 3,
    'WIPRO.BSE': 4,
}

ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_KEY')


def fetch_stock_price(symbol: str) -> dict | None:
    """Fetch latest price for a stock from Alpha Vantage."""
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': ALPHA_VANTAGE_KEY,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        quote = data.get('Global Quote', {})

        if not quote or not quote.get('05. price'):
            print(f"No data for {symbol}")
            return None
        
        return {
            'symbol': symbol.replace('.BSE', ''),
            'price': float(quote['05. price']),
            'change_percent': quote['10. change percent'].replace('%', ''),
            'volume': int(quote['06. volume']),
            'trading_day': quote['07. latest trading day'],
            'timestamp': datetime.now(UTC).isoformat()

        }
    
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None
    

def create_producer() -> KafkaProducer:
    """Create and return a Kafka Producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
    )

def run_producer():
    """Main producer loop - fetch and publish every 60 seconds."""
    print("Starting stock price producer...")
    print(f"Connected to Kafka: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC}")
    print(f"Tracking: {', '.join(STOCKS.keys())}")
    print("=" * 50)

    producer = create_producer()

    while True:
        print(f"\n {datetime.now(UTC).strftime('%H:%M:%S')} — fetching prices...")

        for symbol, partition in STOCKS.items():
            record = fetch_stock_price(symbol)

            if record:
                # Send to kafka - key=symbol ensures same stock -> same partition
                producer.send(
                    topic=TOPIC,
                    key=symbol,
                    value=record,
                    partition=partition,
                )
                print(f"{record['symbol']}: ₹{record['price']} → partition {partition}")

            time.sleep(13) # Rate limit: 5 calls/min on free tier

        producer.flush() # ensure all messages are sent
        print(f"\n Sleeping 60s before next fetch...")
        time.sleep(60)


if __name__ == '__main__':
    run_producer()
     
