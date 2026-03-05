"""
Kafka Producer Module.

Usage from another file::

    from kafka_producer import MSKProducer

    producer = MSKProducer(
        bootstrap_servers="b-1.broker:9094,b-2.broker:9094",
        topic="upi-transactions",
        username="your_username",
        password="your_password",
    )

    producer.send({"user_id": 101, "event": "purchase", "amount": 250})
    producer.send({"user_id": 102, "event": "refund", "amount": 80}, key="user-102")

    producer.close()
"""

import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

logger = logging.getLogger(__name__)


class MSKProducer:
    """Kafka producer for Amazon MSK with SASL_SSL authentication."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        username: str,
        password: str,
        retries: int = 3,
        retry_backoff_s: float = 1.0,
        acks: str = "all",
        compression_type: str = "snappy",
    ) -> None:
        self._topic = topic
        self._retries = retries
        self._retry_backoff_s = retry_backoff_s

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username=username,
                sasl_plain_password=password,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
                acks=acks,
                retries=retries,
                compression_type=compression_type,
            )
            logger.info("Connected to Kafka at %s", bootstrap_servers)
        except NoBrokersAvailable:
            logger.exception("No brokers available at %s", bootstrap_servers)
            raise

    def send(self, data: dict, *, key: str | None = None, topic: str | None = None) -> bool:
        """Send a JSON message to Kafka.

        Args:
            data:  Dictionary payload to publish.
            key:   Optional message key for partitioning.
            topic: Override the default topic.

        Returns:
            True if delivered successfully, False otherwise.
        """
        target = topic or self._topic

        for attempt in range(1, self._retries + 1):
            try:
                future = self._producer.send(target, value=data, key=key)
                metadata = future.get(timeout=30)
                logger.info(
                    "Delivered → topic=%s partition=%s offset=%s",
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )
                return True

            except (KafkaTimeoutError, KafkaError) as exc:
                logger.warning(
                    "Send failed (attempt %d/%d): %s", attempt, self._retries, exc
                )
                if attempt < self._retries:
                    time.sleep(self._retry_backoff_s * (2 ** (attempt - 1)))

        logger.error("Message delivery failed after %d attempts.", self._retries)
        return False

    def close(self) -> None:
        """Flush pending messages and close the producer."""
        try:
            self._producer.flush(timeout=10)
        except KafkaTimeoutError:
            logger.warning("Flush timed out — some messages may not have been delivered.")
        finally:
            self._producer.close()
            logger.info("Kafka producer closed.")
