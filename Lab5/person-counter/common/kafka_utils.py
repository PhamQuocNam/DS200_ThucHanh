"""
common/kafka_utils.py
Tiện ích Kafka dùng chung cho tất cả các server.
"""
import json
import logging
from typing import Any, Callable, Optional
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Tạo Kafka Producer với serializer JSON + base64 cho binary."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_request_size=10 * 1024 * 1024,   # 10 MB
        retries=5,
        acks="all",                            # đảm bảo độ bền dữ liệu
    )


def create_consumer(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = "latest",
) -> KafkaConsumer:
    """Tạo Kafka Consumer với deserializer JSON."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        max_partition_fetch_bytes=10 * 1024 * 1024,
    )


def safe_send(producer: KafkaProducer, topic: str, payload: Any) -> bool:
    """Gửi message lên Kafka, trả về True nếu thành công."""
    try:
        future = producer.send(topic, value=payload)
        future.get(timeout=10)
        return True
    except KafkaError as exc:
        logger.error("Kafka send error: %s", exc)
        return False
