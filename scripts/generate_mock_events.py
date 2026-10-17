import json
import random
import time
import uuid
import argparse
from datetime import datetime, timedelta

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ods_user_behavior"

EVENT_TYPES = ["view", "cart", "order", "pay"]
CHANNELS = ["app", "h5", "wechat", "search", "ad"]
PRODUCT_IDS = [1001, 1002, 1003, 1004, 1005, 1006]
SHOP_IDS = [1, 2, 3, 4, 5]


def build_event(event_time_offset=0):
    event_type = random.choices(EVENT_TYPES, weights=[70, 15, 10, 5], k=1)[0]
    amount = 0.0
    if event_type in ("order", "pay"):
        amount = round(random.uniform(29, 799), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 50000),
        "product_id": random.choice(PRODUCT_IDS),
        "shop_id": random.choice(SHOP_IDS),
        "event_type": event_type,
        "channel": random.choice(CHANNELS),
        "amount": amount,
        "event_time": (datetime.now() + timedelta(seconds=event_time_offset)).strftime("%Y-%m-%d %H:%M:%S"),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Generate mock ecommerce behavior events.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--count", type=int, default=0, help="0 means keep running.")
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--event-time-offset", type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"writing mock events to Kafka topic: {args.topic}")
    sent = 0
    while args.count <= 0 or sent < args.count:
        event = build_event(args.event_time_offset)
        producer.send(args.topic, event)
        print(event)
        sent += 1
        time.sleep(args.interval)
    producer.flush()
    print(f"done, sent {sent} events.")


if __name__ == "__main__":
    main()
