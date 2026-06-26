import json
import random
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
import psycopg2

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "pipeline_source",
    "user": "admin",
    "password": "password"
}

pg_conn = None

def get_ist_time():
    return datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()

active_orders = {}

def generate_order():
    # if there are active orders, progress one of them
    if active_orders and random.random() > 0.3:
        # print(f"inside : if active_orders and random.random() > 0.3")
        order_id = random.choice(list(active_orders.keys()))
        order = active_orders[order_id].copy()
        order["ts"] = get_ist_time()

        if order["status"] == "placed":
            # placed can go to shipped or cancelled
            if random.random() > 0.2:
                order["status"] = "shipped"
                active_orders[order_id]["status"] = "shipped"
            else:
                order["status"] = "cancelled"
                active_orders.pop(order_id)

        elif order["status"] == "shipped":
            # shipped can go to delivered or returned
            if random.random() > 0.3:
                order["status"] = "delivered"
            else:
                order["status"] = "returned"
            active_orders.pop(order_id)

        return order

    # otherwise place a new order
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

def get_connection():
    global pg_conn
    max_retries = 5
    retry_delay = 3

    for attempt in range(max_retries):
        try:
            if pg_conn is not None:
                try:
                    pg_conn.close()
                except:
                    pass
            pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
            print(f"Postgres connected (attempt {attempt + 1})")
            return pg_conn
        except psycopg2.OperationalError as e:
            print(f"Postgres connection failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Could not connect to Postgres after {max_retries} attempts") from e

conn = get_connection()
cur = conn.cursor()

while True:
    order = generate_order()
    print(order)
    try:
        with conn.cursor() as cur:
            if order["status"] == "placed":
                cur.execute("""
                    INSERT INTO orders (order_id, user_id, ts, amount, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    order["order_id"],
                    order["user_id"],
                    order["ts"],
                    order["amount"],
                    order["status"]
                ))
            else:
                cur.execute("""
                    UPDATE orders
                    SET status = %s, ts = %s
                    WHERE order_id = %s
                """, (
                    order["status"],
                    order["ts"],
                    order["order_id"]
                ))

        conn.commit()

    except psycopg2.OperationalError:
        conn = get_connection()

    time.sleep(0.5)