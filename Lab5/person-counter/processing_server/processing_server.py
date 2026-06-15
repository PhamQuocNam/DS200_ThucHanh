"""
processing_server/processing_server.py
─────────────────────────────────────────────────────────────────────────────
Nhiệm vụ:
  1. Consume frame từ Kafka topic `raw-frames`.
  2. Decode base64 → ảnh OpenCV.
  3. Chạy YOLOv8 để phát hiện người (class 0 = person).
  4. Publish kết quả (bounding boxes + count) lên Kafka topic `detection-results`.

Kafka message schema (output):
  {
    "frame_id":      <str>,
    "timestamp":     <float>,
    "camera_id":     <str>,
    "processed_at":  <float>,
    "person_count":  <int>,
    "bounding_boxes": [
      {
        "x1": <int>, "y1": <int>, "x2": <int>, "y2": <int>,
        "confidence": <float>,
        "label": "person"
      }, ...
    ]
  }
"""

import base64
import logging
import os
import time

import cv2
import numpy as np
from ultralytics import YOLO

import sys
sys.path.insert(0, "/app")
from common.kafka_utils import create_consumer, create_producer, safe_send

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_FRAMES     = os.getenv("KAFKA_TOPIC_FRAMES",   "raw-frames")
TOPIC_RESULTS    = os.getenv("KAFKA_TOPIC_RESULTS",  "detection-results")
GROUP_ID         = os.getenv("KAFKA_GROUP_ID",       "processing-group")
CONFIDENCE       = float(os.getenv("MODEL_CONFIDENCE", "0.5"))
MODEL_NAME       = os.getenv("YOLO_MODEL", "yolov8n.pt")   # nano – nhẹ & nhanh

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ProcessingServer] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ─── Model ───────────────────────────────────────────────────────────────────

def load_model() -> YOLO:
    logger.info("Đang tải model YOLOv8: %s", MODEL_NAME)
    model = YOLO(MODEL_NAME)
    logger.info("Model đã sẵn sàng.")
    return model


# ─── Detection ───────────────────────────────────────────────────────────────

def b64_to_frame(b64_str: str) -> np.ndarray:
    """Base64 JPEG → numpy array BGR."""
    data = base64.b64decode(b64_str)
    arr  = np.frombuffer(data, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def detect_persons(model: YOLO, frame: np.ndarray, conf: float) -> list[dict]:
    """
    Chạy YOLOv8, lọc class 0 (person).
    Trả về danh sách bounding boxes.
    """
    results = model(frame, conf=conf, classes=[0], verbose=False)
    boxes = []
    for r in results:
        for box in r.boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
            boxes.append({
                "x1":         x1,
                "y1":         y1,
                "x2":         x2,
                "y2":         y2,
                "confidence": round(float(box.conf[0]), 4),
                "label":      "person",
            })
    return boxes


# ─── Main Loop ───────────────────────────────────────────────────────────────

def main():
    logger.info("Khởi động Processing Server...")
    model    = load_model()
    consumer = create_consumer(KAFKA_BOOTSTRAP, TOPIC_FRAMES, GROUP_ID)
    producer = create_producer(KAFKA_BOOTSTRAP)

    processed = 0
    logger.info("Đang lắng nghe topic '%s'...", TOPIC_FRAMES)

    try:
        for msg in consumer:
            payload = msg.value                     # đã được JSON-decoded bởi consumer
            frame_id  = payload.get("frame_id", "unknown")
            camera_id = payload.get("camera_id", "unknown")
            b64       = payload.get("frame_b64")

            if not b64:
                logger.warning("frame_id=%s thiếu frame_b64, bỏ qua.", frame_id)
                continue

            t0    = time.time()
            frame = b64_to_frame(b64)
            boxes = detect_persons(model, frame, CONFIDENCE)
            elapsed = round(time.time() - t0, 4)

            result = {
                "frame_id":       frame_id,
                "timestamp":      payload.get("timestamp", t0),
                "camera_id":      camera_id,
                "processed_at":   time.time(),
                "inference_time": elapsed,
                "person_count":   len(boxes),
                "bounding_boxes": boxes,
            }

            if safe_send(producer, TOPIC_RESULTS, result):
                processed += 1
                logger.info(
                    "frame_id=%-36s | người=%d | thời_gian=%.3fs | tổng=%d",
                    frame_id, len(boxes), elapsed, processed,
                )

    except KeyboardInterrupt:
        logger.info("Processing Server dừng.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
