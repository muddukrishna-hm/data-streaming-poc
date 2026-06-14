from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from pyflink.datastream.functions import MapFunction

from models import ErrorMetric


class ThresholdConfig:
    def __init__(self, default_threshold: float, services: Dict[str, float]):
        self.default_threshold = default_threshold
        self.services = services

    def for_service(self, service: str) -> float:
        return self.services.get(service, self.default_threshold)

    @classmethod
    def load(cls, path: Path) -> "ThresholdConfig":
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return cls(
            default_threshold=float(data.get("default_threshold", 0.05)),
            services={k: float(v) for k, v in (data.get("services") or {}).items()},
        )


class ThresholdEvaluator(MapFunction):
    """Split ErrorMetric into [METRIC] or [ALERT] console lines."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config: ThresholdConfig | None = None

    def open(self, runtime_context) -> None:
        self._config = ThresholdConfig.load(Path(self.config_path))

    def map(self, metric: ErrorMetric) -> str:
        assert self._config is not None
        threshold = self._config.for_service(metric.service)
        prefix = "ALERT" if metric.error_rate > threshold else "METRIC"
        return f"[{prefix}] {metric} threshold={threshold:.4f}"
