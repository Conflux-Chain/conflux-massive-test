import time
from typing import Callable, TypeVar

from loguru import logger
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException

T = TypeVar("T")

DEFAULT_RETRY_PATTERNS = (
    "RequestLimitExceeded",
    # "InternalError",
    # "TryAgainLater",
)
PENDING_DELETE_RETRY_PATTERNS = (
    "UnsupportedOperation.InstanceStatePending",
)


def _matches_patterns(value: str, patterns: tuple[str, ...]) -> bool:
    return any(pattern and pattern in value for pattern in patterns)


def is_retryable_tencent_error(
    exc: TencentCloudSDKException,
    retry_patterns: tuple[str, ...],
) -> bool:
    code = exc.code or ""
    message = exc.message or ""
    return _matches_patterns(code, retry_patterns) or _matches_patterns(message, retry_patterns)


def call_with_retry(
    operation: Callable[[], T],
    *,
    action: str,
    retry_patterns: tuple[str, ...] = DEFAULT_RETRY_PATTERNS,
    max_attempts: int = 8,
    initial_delay: float = 2.0,
    max_delay: float = 30.0,
) -> T:
    delay = initial_delay
    attempt = 1
    while True:
        try:
            return operation()
        except TencentCloudSDKException as exc:
            if attempt >= max_attempts or not is_retryable_tencent_error(exc, retry_patterns):
                raise

            code = exc.code or "unknown"
            message = exc.message or str(exc)
            logger.warning(
                f"{action} hit retryable Tencent API error {code}: {message}; retry {attempt}/{max_attempts} in {delay:.1f}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
            attempt += 1