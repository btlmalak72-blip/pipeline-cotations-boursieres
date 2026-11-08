import json
import re
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import urlopen

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from redis import Redis


def query(sql):
    with urlopen("http://questdb:9000/exec?" + urlencode({"query": sql}), timeout=10) as response:
        return json.load(response)


def save_quote(record, value):
    symbol = value["symbol"]
    price = Decimal(str(value["price"]))
    if not re.fullmatch(r"[A-Z0-9.\-]{1,20}", symbol) or not price.is_finite() or price <= 0:
        raise ValueError("Symbole ou prix invalide")
    timestamp = datetime.fromisoformat(value["event_time"])
    if timestamp.utcoffset() is None:
        raise ValueError("Horodatage sans fuseau")
    query(
        f"INSERT INTO quotes VALUES ('{symbol}', {price}, '{timestamp.isoformat()}', "
        f"{record.partition}, {record.offset})"
    )


def save_candle(value):
    symbol = value["symbol"]
    start = datetime.fromisoformat(value["window_start"]).isoformat()
    prices = [Decimal(str(value[key])) for key in
              ("open_price", "high_price", "low_price", "close_price")]
    if not re.fullmatch(r"[A-Z0-9.\-]{1,20}", symbol) or any(
        not price.is_finite() or price <= 0 for price in prices
    ):
        raise ValueError("Chandelier invalide")
    query(
        f"INSERT INTO candles_flink VALUES ('{symbol}', '{start}', "
        + ", ".join(map(str, prices)) + ")"
    )


def main():
    query(
        "CREATE TABLE IF NOT EXISTS quotes (symbol SYMBOL, price DOUBLE, event_time TIMESTAMP, "
        "kafka_partition INT, kafka_offset LONG) TIMESTAMP(event_time) PARTITION BY DAY WAL "
        "DEDUP UPSERT KEYS(event_time, kafka_partition, kafka_offset)"
    )
    query(
        "CREATE TABLE IF NOT EXISTS candles_flink (symbol SYMBOL, window_start TIMESTAMP, "
        "open_price DOUBLE, high_price DOUBLE, low_price DOUBLE, close_price DOUBLE) "
        "TIMESTAMP(window_start) PARTITION BY DAY WAL DEDUP UPSERT KEYS(window_start, symbol)"
    )
    redis = Redis(host="redis", socket_timeout=5)
    consumer = KafkaConsumer(
        "quotes.raw", "quotes.minute", bootstrap_servers="redpanda:9092",
        group_id="cotations-stockage-v1", auto_offset_reset="earliest", enable_auto_commit=False,
    )
    try:
        for record in consumer:
            value = json.loads(record.value)
            if record.topic == "quotes.raw":
                save_quote(record, value)
            else:
                has_candle = "open_price" in value
                if has_candle:
                    save_candle(value)
                # Atomique avec XADD pour éviter de republier la même fenêtre.
                redis.eval(
                    "if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then "
                    "redis.call('XADD', KEYS[1], 'MAXLEN', '~', 1000, '*', 'json', ARGV[2]); "
                    "redis.call('SADD', KEYS[2], ARGV[1]); end",
                    2, "quotes:minute", "quotes:minute:seen",
                    f"{'ohlc:' if has_candle else ''}{value['symbol']}:{value['window_start']}", record.value,
                )
            consumer.commit({
                TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1, "", -1)
            })
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
