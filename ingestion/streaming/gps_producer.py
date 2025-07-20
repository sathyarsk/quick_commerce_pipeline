from kafka import KafkaProducer
import json
import time
import random
import csv
import os

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

couriers = [f"C{str(i).zfill(3)}" for i in range(1, 1001)]

csv_file = 'gps_streams.csv'
write_header = not os.path.exists(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["courier_id", "lat", "lon", "ts"])

    if write_header:
        writer.writeheader()

    while True:
        event = {
            "courier_id": random.choice(couriers),
            "lat": round(random.uniform(-90, 90), 6),
            "lon": round(random.uniform(-180, 180), 6),
            "ts": int(time.time())
        }

        producer.send("gps_streams", value=event)
        print("Sent:", event)

        writer.writerow(event)
        file.flush()  

        time.sleep(1)
