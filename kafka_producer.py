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
from config import EMAIL_CONFIG, DOWNLOAD_BASE_URL

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
        security_protocol: str,
        sasl_mechanism: str,
        receiver_mails: str,
        report_name: str,
        status: str,
        run_id: str,
        retries: int = 3,
        retry_backoff_s: float = 5.0,
        acks: str = "all",
        connection_retries: int = 5,
        connection_retry_backoff_s: float = 5.0,
    ) -> None:
        self._topic = topic
        self._retries = retries
        self._retry_backoff_s = retry_backoff_s

        self._producer = self._create_producer_with_retry(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            username=username,
            password=password,
            acks=acks,
            retries=retries,
            max_attempts=connection_retries,
            backoff_s=connection_retry_backoff_s,
        )

        self.status = status
        self.receiver_mails = receiver_mails
        self.report_name = report_name
        self.run_id = run_id

    @staticmethod
    def _create_producer_with_retry(
        bootstrap_servers: str,
        security_protocol: str,
        sasl_mechanism: str,
        username: str,
        password: str,
        acks: str,
        retries: int,
        max_attempts: int,
        backoff_s: float,
    ) -> KafkaProducer:
        """Attempt to create a KafkaProducer with exponential backoff.

        Retries on NoBrokersAvailable and generic connection errors
        that are common with transient MSK connectivity issues.
        """
        last_exception = None

        for attempt in range(1, max_attempts + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(","),
                    security_protocol=security_protocol,
                    sasl_mechanism=sasl_mechanism,
                    sasl_plain_username=username,
                    sasl_plain_password=password,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
                    acks=acks,
                    retries=retries,
                )
                logger.info(
                    "Connected to Kafka at %s (attempt %d/%d)",
                    bootstrap_servers, attempt, max_attempts,
                )
                return producer

            except NoBrokersAvailable as exc:
                last_exception = exc
                logger.warning(
                    "No brokers available at %s (attempt %d/%d): %s",
                    bootstrap_servers, attempt, max_attempts, exc,
                )
            except KafkaError as exc:
                last_exception = exc
                logger.warning(
                    "Kafka connection error at %s (attempt %d/%d): %s",
                    bootstrap_servers, attempt, max_attempts, exc,
                )
            except Exception as exc:
                last_exception = exc
                logger.warning(
                    "Unexpected error connecting to %s (attempt %d/%d): %s",
                    bootstrap_servers, attempt, max_attempts, exc,
                )

            if attempt < max_attempts:
                wait = backoff_s * (2 ** (attempt - 1))
                logger.info("Retrying connection in %.1fs ...", wait)
                time.sleep(wait)

        logger.error(
            "Failed to connect to Kafka after %d attempts.", max_attempts
        )
        raise last_exception

    def send(self, email_body: str | None = None, *, key: str | None = None, topic: str | None = None) -> bool:
        """Send a JSON message to Kafka.

        Args:
            email_body: Optional override for the email body text.
            key:        Optional message key for partitioning.
            topic:      Override the default topic.

        Returns:
            True if delivered successfully, False otherwise.
        """
        target = topic or self._topic

        if self.status.lower() in ["completed", "success", "successfull"]:
            body = (
                f" Hi Team!\n\n"
                f" Report generation {self.status} for {self.report_name}\n\n"
                f" Download URL : {DOWNLOAD_BASE_URL}{self.run_id}/\n\n"
                f" Regards,\n IDP"
            )
        else:
            body = (
                f" Hi Team!\n\n"
                f" Report generation {self.status} for {self.report_name}\n\n"
                f" Regards,\n IDP"
            )

        body = email_body or body

        print(body)

        data = {
            "sender_mail": EMAIL_CONFIG["sender_mail"],
            "receiver_mails": self.receiver_mails,
            "cc_reciever_mail": EMAIL_CONFIG["cc_reciever_mail"],
            "bcc_reciever_mail": EMAIL_CONFIG["bcc_reciever_mail"],
            "mail_subject": f"Report Generation {self.status} for {self.report_name}: Run ID {self.run_id}",
            "email_body": body,
            "body_text_type": EMAIL_CONFIG["body_text_type"],
            "attachment_path": EMAIL_CONFIG["attachment_path"],
        }
        print(data)

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
