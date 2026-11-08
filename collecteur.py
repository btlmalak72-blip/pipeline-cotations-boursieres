import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect


def main():
    key = os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise SystemExit("Renseigne MASSIVE_API_KEY dans ton environnement.")
    producer = KafkaProducer(bootstrap_servers="redpanda:9092")
    try:
        while True:
            try:
                with connect("wss://socket.massive.com/stocks") as socket:
                    socket.send(json.dumps({"action": "auth", "params": key}))
                    for message in socket:
                        for event in json.loads(message):
                            if event.get("status") == "auth_success":
                                socket.send('{"action":"subscribe","params":"T.AAPL"}')
                            if event.get("status") in {"auth_failed", "error", "not_authorized"}:
                                raise SystemExit("Massive a refusé l'accès au flux. Vérifie la clé et le forfait.")
                            if event.get("ev") == "T":
                                timestamp = datetime.fromtimestamp(event["t"] / 1000, timezone.utc)
                                payload = json.dumps({
                                    "symbol": event["sym"], "price": event["p"],
                                    "event_time": timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                                })
                                producer.send("quotes.raw", key=event["sym"].encode(), value=payload.encode()).get(timeout=10)
            except (OSError, ConnectionClosed):
                print("Flux interrompu, reconnexion dans 5 secondes.")
            time.sleep(5)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
