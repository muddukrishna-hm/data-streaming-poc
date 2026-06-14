#!/usr/bin/env python3
"""PyFlink job: read events from a JSONL file, compute 60s error rates, emit alerts."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyflink.common import Configuration, Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.functions import (
    AggregateFunction,
    KeySelector,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows

from alert_engine import ThresholdEvaluator
from models import ApiEvent, ErrorMetric

JOB_DIR = Path(__file__).resolve().parent
REPO_ROOT = JOB_DIR.parent
CONFIG_PATH = Path(os.environ.get("THRESHOLDS_PATH", REPO_ROOT / "config" / "thresholds.json"))
EVENTS_PATH = os.environ.get("EVENTS_PATH", "/opt/flink/events/events.jsonl")

FLINK_REST_HOST = os.environ.get("FLINK_REST_HOST", "localhost")
FLINK_REST_PORT = int(os.environ.get("FLINK_REST_PORT", "8081"))
EXECUTION_TARGET = os.environ.get("EXECUTION_TARGET", "remote")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "60"))
WATERMARK_DELAY_SECONDS = int(os.environ.get("WATERMARK_DELAY_SECONDS", "5"))
FILE_POLL_SECONDS = int(os.environ.get("FILE_POLL_SECONDS", "1"))

EVENT_TYPE = Types.PICKLED_BYTE_ARRAY()


class ParseApiEvent(MapFunction):
    def map(self, value: str) -> ApiEvent:
        data = json.loads(value)
        return ApiEvent(
            service=data["service"],
            status_code=int(data["status_code"]),
            latency_ms=int(data["latency_ms"]),
            timestamp=int(data["timestamp"]),
        )


class ServiceKey(KeySelector):
    def get_key(self, value: ApiEvent) -> str:
        return value.service


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: ApiEvent, record_timestamp: int) -> int:
        return value.timestamp


class ErrorRateAggregator(AggregateFunction):
    def create_accumulator(self) -> tuple[int, int]:
        return (0, 0)

    def add(self, value: ApiEvent, accumulator: tuple[int, int]) -> tuple[int, int]:
        errors, total = accumulator
        total += 1
        if value.is_error():
            errors += 1
        return (errors, total)

    def get_result(self, accumulator: tuple[int, int]) -> tuple[int, int]:
        return accumulator

    def merge(self, acc_a: tuple[int, int], acc_b: tuple[int, int]) -> tuple[int, int]:
        return (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1])


class ErrorMetricEmitter(ProcessWindowFunction):
    def process(self, key: str, context, elements):
        errors, total = elements[0]
        if total == 0:
            return
        window = context.window()
        yield ErrorMetric(
            service=key,
            error_count=errors,
            total_count=total,
            error_rate=errors / total,
            window_start=window.start,
            window_end=window.end,
        )


def build_env() -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string("pipeline.name", "api-error-rate-monitor")

    if EXECUTION_TARGET == "local":
        config.set_string("python.execution-mode", "thread")
        config.set_string("python.executable", "/usr/local/bin/python")
        config.set_string("python.client.executable", "/usr/local/bin/python")
    else:
        config.set_string("python.execution-mode", "process")
        config.set_string("python.executable", "/usr/bin/python3")
        config.set_string("python.client.executable", "/usr/bin/python3")
        if EXECUTION_TARGET == "remote":
            config.set_string("rest.address", FLINK_REST_HOST)
            config.set_integer("rest.port", FLINK_REST_PORT)
        python_files = ",".join(
            str(path) for path in (JOB_DIR / "models.py", JOB_DIR / "alert_engine.py")
        )
        config.set_string("python.files", python_files)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)
    return env


def build_pipeline(env: StreamExecutionEnvironment) -> None:
    file_source = (
        FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            EVENTS_PATH,
        )
        .monitor_continuously(Duration.of_seconds(FILE_POLL_SECONDS))
        .build()
    )

    source = env.from_source(
        file_source,
        WatermarkStrategy.no_watermarks(),
        "file-events",
        Types.STRING(),
    ).set_parallelism(1)

    events = source.map(ParseApiEvent(), output_type=EVENT_TYPE)

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(WATERMARK_DELAY_SECONDS)
        ).with_timestamp_assigner(EventTimestampAssigner())
    )
    watermarked = events.assign_timestamps_and_watermarks(watermark_strategy)

    keyed = watermarked.key_by(ServiceKey(), key_type=Types.STRING())
    metrics = (
        keyed.window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .aggregate(
            ErrorRateAggregator(),
            ErrorMetricEmitter(),
            output_type=EVENT_TYPE,
        )
    )

    output = metrics.map(
        ThresholdEvaluator(str(CONFIG_PATH)),
        output_type=Types.STRING(),
    )
    output.print()


def main() -> None:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"Missing thresholds config: {CONFIG_PATH}")

    events_file = Path(EVENTS_PATH)
    if not events_file.exists():
        raise SystemExit(f"Missing events file: {EVENTS_PATH}")

    sys.path.insert(0, str(JOB_DIR))
    env = build_env()
    build_pipeline(env)
    env.execute("api-error-rate-monitor")


if __name__ == "__main__":
    main()
