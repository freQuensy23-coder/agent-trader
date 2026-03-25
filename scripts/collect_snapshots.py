"""Fetch static HL metadata snapshots for the backtest proxy."""

import json
import sys
from pathlib import Path

import httpx

OUTPUT_DIR = Path("data/proxy_snapshots")
HL_URL = "https://api.hyperliquid.xyz/info"

ENDPOINTS = [
    ("meta", {"type": "meta"}),
    ("allPerpMetas", {"type": "allPerpMetas"}),
    ("spotMeta", {"type": "spotMeta"}),
]


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=15) as client:
        for name, payload in ENDPOINTS:
            print(f"Fetching {name}...", end=" ")
            resp = client.post(HL_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()

            path = OUTPUT_DIR / f"{name}.json"
            path.write_text(json.dumps(data, indent=2))
            print(f"saved to {path}")

    print(f"\nDone. {len(ENDPOINTS)} snapshots saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
