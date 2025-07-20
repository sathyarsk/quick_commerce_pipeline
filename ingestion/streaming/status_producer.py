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

orders = [str(i) for i in range(1, 1001)]
couriers = [f"C{str(i).zfill(3)}" for i in range(1, 1001)]
statuses = ["ordered", "processing", "shipped", "delivered", "cancelled"]

csv_file = 'status_streams.csv'
write_header = not os.path.exists(csv_file)

with open(csv_file, 'a', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["order_id", "courier_id", "status", "ts"])

    if write_header:
        writer.writeheader()

    while True:
        event = {
            "order_id": random.choice(orders),
            "courier_id": random.choice(couriers),
            "status": random.choice(statuses),
            "ts": int(time.time())
        }

        producer.send("status_streams", value=event)
        print("Sent:", event)

        writer.writerow(event)
        file.flush()  

        time.sleep(1)
