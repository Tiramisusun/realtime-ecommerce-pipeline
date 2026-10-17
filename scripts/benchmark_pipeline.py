"""压测脚本：测量 Kafka -> Flink SQL(DWD) 链路的端到端延迟和吞吐。

latency   : 以固定速率写入 ODS，消费 DWD Topic，按 event_id 匹配计算延迟分位数
throughput: 尽可能快地写入 N 条事件，统计 Flink 把 N 条全部写到 DWD 所需时间
"""
import argparse
import json
import multiprocessing
import statistics
import threading
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from generate_mock_events import build_event


def dwd_end_offset(consumer):
    tp = TopicPartition("dwd_user_behavior", 0)
    return consumer.end_offsets([tp])[tp]


def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=5,
        batch_size=256 * 1024,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def percentile(values, p):
    values = sorted(values)
    index = min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))
    return values[index]


def run_latency(args):
    sent_at = {}
    received_at = {}
    stop = threading.Event()

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    tp = TopicPartition("dwd_user_behavior", 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    consumer.position(tp)

    def consume():
        while not stop.is_set():
            for records in consumer.poll(timeout_ms=200).values():
                now = time.time()
                for record in records:
                    received_at.setdefault(record.value["event_id"], now)

    worker = threading.Thread(target=consume)
    worker.start()

    producer = make_producer(args.bootstrap_servers)
    total = args.rate * args.duration
    interval = 1.0 / args.rate
    start = time.time()
    for i in range(total):
        target = start + i * interval
        delay = target - time.time()
        if delay > 0:
            time.sleep(delay)
        event = build_event()
        sent_at[event["event_id"]] = time.time()
        producer.send("ods_user_behavior", event)
    producer.flush()

    deadline = time.time() + 30
    while len(received_at) < total and time.time() < deadline:
        time.sleep(0.5)
    stop.set()
    worker.join()
    consumer.close()

    latencies = [(received_at[k] - sent_at[k]) * 1000 for k in sent_at if k in received_at]
    print(json.dumps({
        "mode": "latency",
        "rate_per_sec": args.rate,
        "duration_sec": args.duration,
        "sent": total,
        "received": len(latencies),
        "p50_ms": round(percentile(latencies, 50), 1),
        "p95_ms": round(percentile(latencies, 95), 1),
        "p99_ms": round(percentile(latencies, 99), 1),
        "max_ms": round(max(latencies), 1),
        "mean_ms": round(statistics.mean(latencies), 1),
    }, indent=2))


def produce_batch(bootstrap, count):
    producer = make_producer(bootstrap)
    for _ in range(count):
        producer.send("ods_user_behavior", build_event())
    producer.flush()


def run_throughput(args):
    offset_reader = KafkaConsumer(bootstrap_servers=args.bootstrap_servers)
    base = dwd_end_offset(offset_reader)
    per_producer = args.count // args.producers
    args.count = per_producer * args.producers
    workers = [
        multiprocessing.Process(target=produce_batch, args=(args.bootstrap_servers, per_producer))
        for _ in range(args.producers)
    ]

    start = time.time()
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    produce_done = time.time()

    while True:
        processed = dwd_end_offset(offset_reader) - base
        if processed >= args.count:
            break
        if time.time() - produce_done > 300:
            break
        time.sleep(0.2)
    flink_done = time.time()
    offset_reader.close()

    print(json.dumps({
        "mode": "throughput",
        "events": args.count,
        "producers": args.producers,
        "dwd_processed": processed,
        "produce_sec": round(produce_done - start, 2),
        "producer_events_per_sec": round(args.count / (produce_done - start)),
        "end_to_end_sec": round(flink_done - start, 2),
        "pipeline_events_per_sec": round(processed / (flink_done - start)),
    }, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Kafka -> Flink SQL -> Kafka(DWD) pipeline.")
    parser.add_argument("mode", choices=["latency", "throughput"])
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=200, help="latency mode: events per second")
    parser.add_argument("--duration", type=int, default=30, help="latency mode: seconds")
    parser.add_argument("--count", type=int, default=100000, help="throughput mode: total events")
    parser.add_argument("--producers", type=int, default=1, help="throughput mode: parallel producer processes")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_latency(args) if args.mode == "latency" else run_throughput(args)
