import argparse
import json
import random
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from itertools import cycle

START_PRICES = {"AAPL": 19025, "MSFT": 42000, "NVDA": 13500, "TSLA": 25000}


def main():
    parser = argparse.ArgumentParser(description="Simule des cotations boursières.")
    parser.add_argument("--count", type=int, default=0, help="0 pour tourner en continu")
    parser.add_argument("--symbol", action="append", choices=START_PRICES,
                        help="Symbole à simuler, répétable. Par défaut, tous les symboles.")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count doit être positif ou nul")

    prices = START_PRICES.copy()
    for sent, symbol in enumerate(cycle(args.symbol or START_PRICES), start=1):
        quote = {
            "symbol": symbol,
            "price": prices[symbol] / 100,
            "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }
        payload = json.dumps(quote)
        subprocess.run(
            [
                "docker", "compose", "exec", "-T", "redpanda",
                "rpk", "topic", "produce", "quotes.raw", "--key", symbol,
                "-X", "brokers=redpanda:9092",
            ],
            cwd=Path(__file__).resolve().parent,
            input=payload + "\n",
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        print(payload, flush=True)
        if sent == args.count:
            break
        prices[symbol] = max(1, prices[symbol] + random.randint(-20, 20))
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except subprocess.CalledProcessError as error:
        raise SystemExit(f"Publication échouée (code {error.returncode}).") from None
