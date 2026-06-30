import json
import random
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

def get_ist_time():
    return datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()

active_orders = {}

schema = {
  "type": "record",
  "name": "Order",
  "namespace": "com.pipeline",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "int"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": "string"},
    {"name": "ts", "type": "string"}
  ]
}

def delivery_report(err, msg):
    """ Called once for each message sent to indicate success or failure. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_producer():
    while True:
        try:
            # producer = KafkaProducer(
            #     bootstrap_servers='localhost:9092',
            #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
            # )
            
            producer_config = {
                "bootstrap.servers": "localhost:9092",  # Change to your Kafka broker URL
                "client.id": "avro-producer-client",
                "enable.idempotence": True
                }
            
            producer = Producer(producer_config)

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

topic_name = "orders"
sr_config = {"url": "http://localhost:8081"}  
schema_registry_client = SchemaRegistryClient(sr_config)

avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=json.dumps(schema)
    )

producer = create_producer()

while True:
    order = generate_order()

    message_key = f"user_key_{order['order_id']}"
    serialized_value = avro_serializer(
            order, 
            SerializationContext(topic_name, MessageField.VALUE)
        )

    # producer.send(
    #     'orders',
    #     key=str(order["order_id"]).encode('utf-8'),
    #     value=order
    # )

    producer.produce(
    topic=topic_name,
    key=message_key,
    value=serialized_value,
    on_delivery=delivery_report  
    )
    
    print(f"Sent: {order}")
    producer.poll(0)
    time.sleep(0.5)