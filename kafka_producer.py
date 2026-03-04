"""
Production-ready Kafka Producer for MSK.

Features:
    - Class-based with context manager support
    - Environment-based configuration (no hardcoded secrets)
    - Retry logic with exponential backoff
    - Structured logging
    - Callback-based delivery confirmation
    - Graceful shutdown
    - Health check support
    - Batch and single message publishing
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    NoBrokersAvailable,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KafkaProducerConfig:
    """Immutable configuration loaded from environment variables."""

    bootstrap_servers: str = field(
        default_factory=lambda: os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    )
    topic: str = field(
        default_factory=lambda: os.environ["KAFKA_TOPIC"]
    )
    sasl_username: str = field(
        default_factory=lambda: os.environ["KAFKA_SASL_USERNAME"]
    )
    sasl_password: str = field(
        default_factory=lambda: os.environ["KAFKA_SASL_PASSWORD"]
    )

    # Protocol
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"

    # Performance tuning
    acks: str = "all"
    retries: int = 5
    retry_backoff_ms: int = 300
    linger_ms: int = 10
    batch_size: int = 32_768          # 32 KB
    buffer_memory: int = 67_108_864   # 64 MB
    max_in_flight_requests: int = 5
    compression_type: str = "snappy"
    request_timeout_ms: int = 30_000

    # Application-level retry settings
    max_send_retries: int = 3
    send_retry_backoff_s: float = 1.0

    @classmethod
    def from_env(cls) -> KafkaProducerConfig:
        """Build config from environment, with optional overrides."""
        overrides: dict[str, Any] = {}
        if val := os.getenv("KAFKA_COMPRESSION_TYPE"):
            overrides["compression_type"] = val
        if val := os.getenv("KAFKA_ACKS"):
            overrides["acks"] = val
        if val := os.getenv("KAFKA_LINGER_MS"):
            overrides["linger_ms"] = int(val)
        return cls(**overrides)


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class MSKProducer:
    """Thread-safe, production-grade Kafka producer for Amazon MSK.

    Usage::

        config = KafkaProducerConfig.from_env()
        with MSKProducer(config) as producer:
            producer.send_message({"user_id": 101, "event": "purchase", "amount": 250})
    """

    def __init__(
        self,
        config: KafkaProducerConfig,
        value_serializer: Callable[[Any], bytes] | None = None,
        key_serializer: Callable[[Any], bytes] | None = None,
    ) -> None:
        self._config = config
        self._producer: KafkaProducer | None = None
        self._running = False

        self._value_serializer = value_serializer or (
            lambda v: json.dumps(v, default=str).encode("utf-8")
        )
        self._key_serializer = key_serializer or (
            lambda k: k.encode("utf-8") if isinstance(k, str) else json.dumps(k).encode("utf-8")
        )

    # -- Lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        """Create the underlying KafkaProducer with full configuration."""
        if self._producer is not None:
            logger.warning("Producer already connected; skipping reconnect.")
            return

        logger.info(
            "Connecting to Kafka brokers at %s …", self._config.bootstrap_servers
        )
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers.split(","),
                security_protocol=self._config.security_protocol,
                sasl_mechanism=self._config.sasl_mechanism,
                sasl_plain_username=self._config.sasl_username,
                sasl_plain_password=self._config.sasl_password,
                value_serializer=self._value_serializer,
                key_serializer=self._key_serializer,
                acks=self._config.acks,
                retries=self._config.retries,
                retry_backoff_ms=self._config.retry_backoff_ms,
                linger_ms=self._config.linger_ms,
                batch_size=self._config.batch_size,
                buffer_memory=self._config.buffer_memory,
                max_in_flight_requests_per_connection=self._config.max_in_flight_requests,
                compression_type=self._config.compression_type,
                request_timeout_ms=self._config.request_timeout_ms,
            )
            self._running = True
            logger.info("Kafka producer connected successfully.")
        except NoBrokersAvailable:
            logger.exception("No Kafka brokers available at %s", self._config.bootstrap_servers)
            raise

    def close(self, timeout: float = 10.0) -> None:
        """Flush pending messages and release resources."""
        self._running = False
        if self._producer is None:
            return
        logger.info("Closing Kafka producer (flush timeout=%.1fs) …", timeout)
        try:
            self._producer.flush(timeout=timeout)
        except KafkaTimeoutError:
            logger.warning("Flush timed out — some messages may be lost.")
        finally:
            self._producer.close(timeout=timeout)
            self._producer = None
            logger.info("Kafka producer closed.")

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> MSKProducer:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.close()

    # -- Publishing ----------------------------------------------------------

    def send_message(
        self,
        value: Any,
        *,
        key: str | None = None,
        topic: str | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> bool:
        """Publish a single message with application-level retry.

        Returns ``True`` if the message was accepted by the broker.
        """
        target_topic = topic or self._config.topic
        self._ensure_connected()

        for attempt in range(1, self._config.max_send_retries + 1):
            try:
                future = self._producer.send(
                    target_topic,
                    value=value,
                    key=key,
                    headers=headers,
                )
                metadata = future.get(timeout=self._config.request_timeout_ms / 1000)
                logger.debug(
                    "Delivered → topic=%s partition=%s offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )
                return True

            except KafkaTimeoutError:
                logger.warning(
                    "Send timeout (attempt %d/%d) for topic=%s",
                    attempt,
                    self._config.max_send_retries,
                    target_topic,
                )
            except KafkaError:
                logger.exception(
                    "Kafka error (attempt %d/%d) for topic=%s",
                    attempt,
                    self._config.max_send_retries,
                    target_topic,
                )

            if attempt < self._config.max_send_retries:
                backoff = self._config.send_retry_backoff_s * (2 ** (attempt - 1))
                time.sleep(backoff)

        logger.error("Failed to deliver message after %d attempts.", self._config.max_send_retries)
        return False

    def send_batch(
        self,
        messages: list[dict[str, Any]],
        *,
        key_field: str | None = None,
        topic: str | None = None,
    ) -> tuple[int, int]:
        """Publish a batch of messages asynchronously, then flush.

        Args:
            messages:  List of dicts to publish.
            key_field: If provided, extract this field from each message as the key.
            topic:     Override the default topic.

        Returns:
            (success_count, failure_count)
        """
        target_topic = topic or self._config.topic
        self._ensure_connected()

        successes = 0
        failures = 0

        def _on_success(metadata):  # noqa: ANN001
            nonlocal successes
            successes += 1
            logger.debug(
                "Batch msg delivered → partition=%s offset=%s",
                metadata.partition,
                metadata.offset,
            )

        def _on_error(exc):  # noqa: ANN001
            nonlocal failures
            failures += 1
            logger.error("Batch msg failed: %s", exc)

        for msg in messages:
            key = msg.get(key_field) if key_field else None
            future = self._producer.send(target_topic, value=msg, key=key)
            future.add_callback(_on_success)
            future.add_errback(_on_error)

        self._producer.flush()
        logger.info("Batch complete: %d sent, %d failed", successes, failures)
        return successes, failures

    # -- Health check --------------------------------------------------------

    def is_healthy(self) -> bool:
        """Return True if the producer is connected and the broker is reachable."""
        if self._producer is None or not self._running:
            return False
        try:
            return self._producer.bootstrap_connected()
        except Exception:
            return False

    # -- Internals -----------------------------------------------------------

    def _ensure_connected(self) -> None:
        if self._producer is None or not self._running:
            raise RuntimeError("Producer is not connected. Call connect() first.")


# ---------------------------------------------------------------------------
# Entrypoint (demo / manual testing)
# ---------------------------------------------------------------------------

def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )


def main() -> None:
    _configure_logging()

    config = KafkaProducerConfig.from_env()

    with MSKProducer(config) as producer:
        # Register graceful shutdown on SIGTERM / SIGINT
        def _shutdown(signum, frame):  # noqa: ANN001
            logger.info("Signal %s received — shutting down.", signum)
            producer.close()
            raise SystemExit(0)

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

        # Single message
        ok = producer.send_message(
            {"user_id": 101, "event": "purchase", "amount": 250},
            key="user-101",
        )
        logger.info("Single send result: %s", "success" if ok else "FAILED")

        # Batch
        batch = [
            {"user_id": i, "event": "purchase", "amount": i * 10}
            for i in range(1, 6)
        ]
        sent, failed = producer.send_batch(batch, key_field="user_id")
        logger.info("Batch result: %d delivered, %d failed", sent, failed)

        # Health check
        logger.info("Producer healthy: %s", producer.is_healthy())


if __name__ == "__main__":
    main()
