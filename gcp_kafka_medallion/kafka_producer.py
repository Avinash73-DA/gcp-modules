import requests
import json
from kafka import KafkaProducer

# Initialize Producer pointing to local docker
EXTERNAL_IP = "10.160.0.2:9092"

producer = KafkaProducer(
    bootstrap_servers=[EXTERNAL_IP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = 'wiki-changes'
url = "https://stream.wikimedia.org/v2/stream/recentchange"
headers = {'User-Agent': 'PythonStreamingTest/1.0'}

print(f"✅ Connecting to Kafka at {EXTERNAL_IP}...")

try:
    with requests.get(url, stream=True, headers=headers) as response:
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data: "):
                    json_data = json.loads(decoded_line[6:]) # Parse string to JSON object

                    try:
                        # Send JSON object directly (serializer handles encoding)
                        producer.send(topic_name, value=json_data)

                        producer.flush() 
                        print("Message sent -> GCP Kafka")
                    except Exception as e:
                        print(f"Error: {e}")
except KeyboardInterrupt:
    print("Stopping producer...")
