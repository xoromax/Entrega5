from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:

    event = {
        "sensor_id": f"sensor_{random.randint(1,5)}",
        "temperature": round(random.uniform(15,35), 2),
        "humidity": round(random.uniform(40,90), 2),
        "air_quality_index": random.randint(10,100),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send(
        "urban_sensors", 
        event
    )

    print(event)

    time.sleep(2)