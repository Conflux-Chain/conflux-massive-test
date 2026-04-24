import os
from dataclasses import dataclass, field
from typing import Any, cast

from .rate_limiter import TencentRateLimitConfig, TencentRateLimiterRegistry
from .retry import DEFAULT_RETRY_PATTERNS, PENDING_DELETE_RETRY_PATTERNS, call_with_retry


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class TencentRetryPolicy:
    retry_patterns: tuple[str, ...] = DEFAULT_RETRY_PATTERNS
    max_attempts: int = 8
    initial_delay: float = 2.0
    max_delay: float = 30.0


@dataclass
class TencentRequestController:
    rate_limit_config: TencentRateLimitConfig
    default_retry_policy: TencentRetryPolicy
    method_retry_policies: dict[tuple[str, str], TencentRetryPolicy] = field(default_factory=dict)
    _rate_limiter_registry: TencentRateLimiterRegistry = field(init=False, repr=False)

    def __post_init__(self):
        self._rate_limiter_registry = TencentRateLimiterRegistry(self.rate_limit_config)

    @classmethod
    def from_env(cls) -> "TencentRequestController":
        base_policy = TencentRetryPolicy(
            max_attempts=max(1, _env_int("TENCENT_API_RETRY_MAX_ATTEMPTS", 8)),
            initial_delay=max(0.1, _env_float("TENCENT_API_RETRY_INITIAL_DELAY", 2.0)),
            max_delay=max(0.1, _env_float("TENCENT_API_RETRY_MAX_DELAY", 30.0)),
        )
        terminate_policy = TencentRetryPolicy(
            retry_patterns=DEFAULT_RETRY_PATTERNS + PENDING_DELETE_RETRY_PATTERNS,
            max_attempts=max(base_policy.max_attempts, 24),
            initial_delay=max(base_policy.initial_delay, 5.0),
            max_delay=max(base_policy.max_delay, 30.0),
        )
        return cls(
            rate_limit_config=TencentRateLimitConfig.from_env(),
            default_retry_policy=base_policy,
            method_retry_policies={
                ("cvm", "TerminateInstances"): terminate_policy,
            },
        )

    def retry_policy_for(self, service_name: str, method_name: str) -> TencentRetryPolicy:
        return self.method_retry_policies.get((service_name, method_name), self.default_retry_policy)

    def wrap_client(self, raw_client: Any, service_name: str) -> Any:
        return TencentApiClientProxy(raw_client, service_name, self)


class TencentApiClientProxy:
    def __init__(self, raw_client: Any, service_name: str, controller: TencentRequestController):
        self._raw_client = raw_client
        self._service_name = service_name
        self._controller = controller

    def __getattr__(self, name: str):
        attr = getattr(self._raw_client, name)
        if not callable(attr) or not name[:1].isupper():
            return attr

        def _wrapped(*args, **kwargs):
            rate_limiter = self._controller._rate_limiter_registry.get(self._service_name)
            retry_policy = self._controller.retry_policy_for(self._service_name, name)

            def _invoke_once():
                if rate_limiter is not None:
                    rate_limiter.acquire()
                return attr(*args, **kwargs)

            return call_with_retry(
                _invoke_once,
                action=f"Tencent {self._service_name}.{name}",
                retry_patterns=retry_policy.retry_patterns,
                max_attempts=retry_policy.max_attempts,
                initial_delay=retry_policy.initial_delay,
                max_delay=retry_policy.max_delay,
            )

        return _wrapped


def wrap_tencent_client(raw_client: Any, service_name: str, controller: TencentRequestController) -> Any:
    return cast(Any, controller.wrap_client(raw_client, service_name))