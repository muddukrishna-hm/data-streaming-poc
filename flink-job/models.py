from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ApiEvent:
    service: str
    status_code: int
    latency_ms: int
    timestamp: int

    def is_error(self) -> bool:
        return 400 <= self.status_code < 600


@dataclass
class ErrorMetric:
    service: str
    error_count: int
    total_count: int
    error_rate: float
    window_start: int
    window_end: int

    def __str__(self) -> str:
        return (
            f"service={self.service} error_rate={self.error_rate:.4f} "
            f"errors={self.error_count}/{self.total_count} "
            f"window=[{self.window_start},{self.window_end})"
        )
