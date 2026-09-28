"""Thin wrapper around confluent-kafka's Producer with delivery tracking."""
import json
import logging

from confluent_kafka import KafkaException, Producer

log = logging.getLogger(__name__)


class WeatherProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "weather-ingestion",
            "acks": "all",                 # wait until the broker has stored the message
            "enable.idempotence": True,    # broker drops duplicates caused by producer retries
            "linger.ms": 50,               # tiny batching window; we send 7 msgs per cycle
            "message.timeout.ms": 30000,   # give up on a message after 30s
        })

    def check_connection(self, timeout=10):
        """Fail fast at startup if Kafka is down or the topic doesn't exist."""
        try:
            metadata = self.producer.list_topics(timeout=timeout)
        except KafkaException as exc:
            raise RuntimeError(f"Cannot reach Kafka: {exc}. Is 'docker compose up -d' running?") from exc
        if self.topic not in metadata.topics:
            raise RuntimeError(f"Kafka topic '{self.topic}' does not exist. Create it first (see Phase 4).")

    def _on_delivery(self, err, msg):
        # Called from poll()/flush() once the broker confirms (or rejects) a message.
        if err is not None:
            self.failed += 1
            log.error("Delivery failed key=%s: %s", msg.key(), err)
        else:
            self.delivered += 1
            log.debug("Delivered key=%s partition=%s offset=%s", msg.key(), msg.partition(), msg.offset())

    def publish(self, event):
        # Key = city, so all events of one city land on the same partition (ordered per city).
        self.producer.produce(
            self.topic,
            key=event["city"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)  # serve delivery callbacks for earlier messages

    def flush(self, timeout=30):
        """Block until all queued messages are delivered; returns how many are still pending."""
        return self.producer.flush(timeout)
