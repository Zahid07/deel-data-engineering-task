#!/usr/bin/env python3
"""Register Debezium connectors once Kafka Connect is ready."""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from pathlib import Path
from urllib import error, request

CONNECT_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "CONNECT_URLS", "http://kafka-connect:8083,http://localhost:8083"
    ).split(",")
    if url.strip()
]
CONNECTORS_DIR = os.getenv("CONNECTORS_DIR", "./debezium/connectors")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "30"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))


def get_status_code(url: str) -> int | None:
    req = request.Request(url, method="GET")
    try:
        with request.urlopen(req, timeout=5) as resp:
            return resp.status
    except Exception:
        return None


def post_json(url: str, payload: bytes) -> tuple[int | None, str]:
    req = request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body
    except Exception as exc:
        return None, str(exc)


def get_text(url: str) -> str:
    req = request.Request(url, method="GET")
    with request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8", errors="replace")


def main() -> int:
    if not CONNECT_URLS:
        print("No CONNECT_URLS provided")
        return 1

    print("Waiting for Kafka Connect to be ready...")

    active_connect_url: str | None = None
    for retry_count in range(MAX_RETRIES):
        for connect_url in CONNECT_URLS:
            status_code = get_status_code(f"{connect_url}/connectors")
            if status_code == 200:
                active_connect_url = connect_url
                print(
                    f"Kafka Connect is ready at {active_connect_url} "
                    f"(HTTP {status_code})"
                )
                break

        if active_connect_url:
            break

        print(
            "Kafka Connect not ready yet "
            f"(tried: {', '.join(CONNECT_URLS)}), "
            f"retrying in {RETRY_DELAY_SECONDS}s... "
            f"({retry_count}/{MAX_RETRIES})"
        )
        time.sleep(RETRY_DELAY_SECONDS)
    else:
        print(f"Failed to connect to Kafka Connect after {MAX_RETRIES} attempts")
        return 1

    if not active_connect_url:
        print("Kafka Connect URL could not be determined")
        return 1

    print("Kafka Connect is ready. Registering connectors...")

    connector_files = sorted(glob.glob(str(Path(CONNECTORS_DIR) / "*.json")))
    if not connector_files:
        print(f"No connector files found in: {CONNECTORS_DIR}")

    for connector_file in connector_files:
        connector_name = Path(connector_file).stem
        print(f"Creating connector from {connector_name}...")

        with open(connector_file, "rb") as fp:
            payload = fp.read()

        # Validate JSON early so malformed files fail fast with clear context.
        try:
            json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            print(f"Invalid JSON in {connector_file}: {exc}")
            continue

        http_code, body = post_json(f"{active_connect_url}/connectors", payload)

        if http_code == 201:
            print(f"Connector {connector_name} created successfully.")
        elif http_code == 409:
            print(f"Connector {connector_name} already exists, skipping.")
        else:
            print(
                f"Failed to create connector {connector_name} "
                f"(HTTP {http_code}): {body}"
            )

    print("\nAll connectors processed. Current status:")
    try:
        print(get_text(f"{active_connect_url}/connectors?expand=status"))
    except Exception as exc:
        print(f"Failed to fetch connector status: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
