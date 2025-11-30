import json
from kafka import KafkaConsumer

# 1. Connect to the Local Kafka Broker
consumer = KafkaConsumer(
    'wiki-changes', # The topic we are listening to
    bootstrap_servers=['xx.xx.xxx.x:9092'],
    auto_offset_reset='latest', # Start reading from now (ignore old stuff)
    value_deserializer=lambda x: x.decode('utf-8') # Decode the raw bytes to string
)

print("✅ Connected to Kafka! Waiting for messages...")
print("---------------------------------------------")

# 2. Loop forever and print messages as they arrive
try:
    for message in consumer:
        # message.value is the raw JSON string we sent
        print(message)
except KeyboardInterrupt:
    print("Stopping consumer...")
