import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from datetime import datetime
from zoneinfo import ZoneInfo

def get_ist_time():
    return datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()

active_orders = {}

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka")
            return producer
        except Exception as e:
            print(f"Kafka not ready, retrying in 3s... ({e})")
            time.sleep(3)

def generate_order():    
    
    order = {
        "user_id": random.randint(1, 100),
        "ts": get_ist_time()
    }

    if order["user_id"] not in active_orders:
        order["order_id"] = str(uuid.uuid4())
        order["amount"] = round(random.uniform(10.0, 500.0), 2)
        order["status"] = "placed"
        
        active_orders[order["user_id"]] = order
        return order

    else:

        order["status"] = random.choice(["cancelled", "returned"])
        order["amount"] = active_orders[order["user_id"]]["amount"]
        order["order_id"] = active_orders[order["user_id"]]["order_id"]

        active_orders.pop(order["user_id"])
        return order
        

producer = create_producer()

while True:
    order = generate_order()
    producer.send('orders',
                   key=str(order["user_id"]).encode('utf-8'),
                   value=order)
    print(f"Sent: {order}")
    time.sleep(0.5)