import os
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Mapping, Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class TencentServiceRateLimitConfig:
    qps: float
    burst: int
    enabled: bool = True


@dataclass(frozen=True)
class TencentRateLimitConfig:
    enabled: bool = True
    default_qps: float = 20.0
    default_burst: int = 5
    service_overrides: Mapping[str, TencentServiceRateLimitConfig] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "TencentRateLimitConfig":
        enabled = _env_bool("TENCENT_API_RATE_LIMIT_ENABLED", True)
        default_qps = _env_float("TENCENT_API_RATE_LIMIT_QPS", 20.0)
        default_burst = max(1, _env_int("TENCENT_API_RATE_LIMIT_BURST", 5))

        service_overrides = {}
        for service_name in ("cvm", "vpc"):
            prefix = f"TENCENT_API_RATE_LIMIT_{service_name.upper()}"
            service_enabled = _env_bool(f"{prefix}_ENABLED", enabled)
            service_qps = _env_float(f"{prefix}_QPS", default_qps)
            service_burst = max(1, _env_int(f"{prefix}_BURST", default_burst))
            service_overrides[service_name] = TencentServiceRateLimitConfig(
                qps=service_qps,
                burst=service_burst,
                enabled=service_enabled,
            )

        return cls(
            enabled=enabled,
            default_qps=default_qps,
            default_burst=default_burst,
            service_overrides=service_overrides,
        )

    def for_service(self, service_name: str) -> Optional[TencentServiceRateLimitConfig]:
        config = self.service_overrides.get(service_name)
        if config is None:
            config = TencentServiceRateLimitConfig(
                qps=self.default_qps,
                burst=self.default_burst,
                enabled=self.enabled,
            )
        if not config.enabled or config.qps <= 0 or config.burst <= 0:
            return None
        return config


class TokenBucketRateLimiter:
    def __init__(self, qps: float, burst: int):
        self._rate = qps
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._updated_at = time.monotonic()
        self._lock = Lock()

    def acquire(self):
        while True:
            sleep_for = 0.0
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated_at
                self._updated_at = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                sleep_for = (1 - self._tokens) / self._rate
            time.sleep(max(sleep_for, 0.001))


class TencentRateLimiterRegistry:
    def __init__(self, config: TencentRateLimitConfig):
        self._config = config
        self._limiters: dict[str, TokenBucketRateLimiter] = {}
        self._lock = Lock()

    def get(self, service_name: str) -> Optional[TokenBucketRateLimiter]:
        service_config = self._config.for_service(service_name)
        if service_config is None:
            return None

        with self._lock:
            limiter = self._limiters.get(service_name)
            if limiter is None:
                limiter = TokenBucketRateLimiter(
                    qps=service_config.qps,
                    burst=service_config.burst,
                )
                self._limiters[service_name] = limiter
            return limiter