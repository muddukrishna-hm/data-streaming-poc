#!/usr/bin/env python3
"""Simulate API response events from four microservices into Kafka."""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass
from typing import Optional

from confluent_kafka import Producer

# Microservices whose API responses we simulate.
SERVICES = ("auth", "order", "pay", "notif")
TOPIC = "api-response-events"


@dataclass
class ApiResponseEvent:
    service: str
    status_code: int
    latency_ms: int
    timestamp: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def delivery_report(err, msg) -> None:
    """Kafka delivery callback; logs failures only (success is silent)."""
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def pick_status_code(service: str, burst_service: Optional[str], burst_error_rate: float) -> int:
    # During a burst, the target service gets a high share of 5xx responses.
    if burst_service == service and random.random() < burst_error_rate:
        return random.choice([500, 502, 503, 504])

    # Steady-state distribution: ~3% 4xx, ~2% 5xx, ~95% success.
    roll = random.random()
    if roll < 0.03:
        return random.choice([400, 401, 403, 404, 429])
    if roll < 0.05:
        return random.choice([500, 502, 503])
    return random.choice([200, 201, 204])


def build_event(service: str, burst_service: Optional[str], burst_error_rate: float) -> ApiResponseEvent:
    return ApiResponseEvent(
        service=service,
        status_code=pick_status_code(service, burst_service, burst_error_rate),
        latency_ms=random.randint(20, 500),
        timestamp=int(time.time() * 1000),
    )


def run(
    bootstrap: str,
    rate_per_service: float,
    burst_service: Optional[str],
    burst_error_rate: float,
    burst_duration_sec: int,
) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    running = True

    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False

    # Graceful shutdown on Ctrl+C or container stop.
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Burst mode temporarily spikes 5xx errors on one service to demo alerting.
    burst_started_at: Optional[float] = None
    if burst_service:
        if burst_service not in SERVICES:
            raise SystemExit(f"--burst must be one of: {', '.join(SERVICES)}")
        burst_started_at = time.time()
        print(
            f"Burst mode: injecting ~{burst_error_rate:.0%} 5xx errors into '{burst_service}' "
            f"for {burst_duration_sec}s"
        )

    # Sleep between ticks so each service emits ~rate_per_service events/s.
    interval = 1.0 / rate_per_service if rate_per_service > 0 else 1.0
    print(f"Producing to topic '{TOPIC}' on {bootstrap} (~{rate_per_service:.1f} events/s per service)")

    while running:
        active_burst = burst_service
        if burst_started_at is not None:
            elapsed = time.time() - burst_started_at
            if elapsed >= burst_duration_sec:
                print(f"Burst ended for '{burst_service}' after {burst_duration_sec}s")
                active_burst = None
                burst_started_at = None

        # One event per service per tick; key partitions by service in Kafka.
        for service in SERVICES:
            event = build_event(service, active_burst, burst_error_rate)
            producer.produce(
                TOPIC,
                key=service.encode("utf-8"),
                value=event.to_json().encode("utf-8"),
                callback=delivery_report,
            )
        # Non-blocking poll drives delivery callbacks without waiting.
        producer.poll(0)
        time.sleep(interval)

    # Block up to 10s so in-flight messages are delivered before exit.
    producer.flush(10)
    print("Producer stopped.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate API response events into Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument(
        "--rate",
        type=float,
        default=8.0,
        help="Events per second per service (default: 8)",
    )
    parser.add_argument(
        "--burst",
        choices=SERVICES,
        default=None,
        help="Temporarily raise 5xx rate for one service (demo alerts)",
    )
    parser.add_argument(
        "--burst-error-rate",
        type=float,
        default=0.40,
        help="Error rate during burst mode (default: 0.40)",
    )
    parser.add_argument(
        "--burst-duration",
        type=int,
        default=120,
        help="Burst duration in seconds (default: 120)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        bootstrap=args.bootstrap,
        rate_per_service=args.rate,
        burst_service=args.burst,
        burst_error_rate=args.burst_error_rate,
        burst_duration_sec=args.burst_duration,
    )
