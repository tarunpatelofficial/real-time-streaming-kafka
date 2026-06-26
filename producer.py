import json
import random
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer

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
    if active_orders and random.random() > 0.3:
        order_id = random.choice(list(active_orders.keys()))
        order = active_orders[order_id].copy()
        order["ts"] = get_ist_time()

        if order["status"] == "placed":
            if random.random() > 0.2:
                order["status"] = "shipped"
                active_orders[order_id]["status"] = "shipped"
            else:
                order["status"] = "cancelled"
                active_orders.pop(order_id)

        elif order["status"] == "shipped":
            if random.random() > 0.3:
                order["status"] = "delivered"
            else:
                order["status"] = "returned"
            active_orders.pop(order_id)

        return order

    user_id = random.randint(1, 100)
    order_id = str(uuid.uuid4())
    amount = round(random.uniform(10.0, 500.0), 2)

    order = {
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "status": "placed",
        "ts": get_ist_time()
    }

    active_orders[order_id] = order.copy()
    return order

producer = create_producer()

while True:
    order = generate_order()
    producer.send(
        'orders',
        key=str(order["order_id"]).encode('utf-8'),
        value=order
    )
    print(f"Sent: {order}")
    time.sleep(0.5)