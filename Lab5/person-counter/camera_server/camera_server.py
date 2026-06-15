"""
camera_server/camera_server.py
─────────────────────────────────────────────────────────────────────────────
Nhiệm vụ:
  1. Nhận khung hình từ camera (webcam / RTSP) HOẶC từ TCP client (tcp_example.py).
  2. Encode frame sang JPEG → base64.
  3. Publish lên Kafka topic `raw-frames`.

Kafka message schema:
  {
    "frame_id":   <str uuid>,
    "timestamp":  <float epoch>,
    "camera_id":  <str>,
    "width":      <int>,
    "height":     <int>,
    "frame_b64":  <str base64-encoded JPEG>
  }
"""

import base64
import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime

import cv2
import numpy as np

import sys
sys.path.insert(0, "/app")
from common.kafka_utils import create_producer, safe_send

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_FRAMES     = os.getenv("KAFKA_TOPIC_FRAMES", "raw-frames")
CAMERA_SOURCE    = os.getenv("CAMERA_SOURCE", "0")          # "0" hoặc URL RTSP
FRAME_RATE       = int(os.getenv("FRAME_RATE", "5"))
FRAME_WIDTH      = int(os.getenv("FRAME_WIDTH", "640"))
FRAME_HEIGHT     = int(os.getenv("FRAME_HEIGHT", "480"))
CAMERA_ID        = os.getenv("CAMERA_ID", "cam-01")
TCP_HOST         = os.getenv("TCP_HOST", "0.0.0.0")
TCP_PORT         = int(os.getenv("TCP_PORT", "6100"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CameraServer] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def frame_to_b64(frame: np.ndarray, quality: int = 80) -> str:
    """Encode OpenCV frame → JPEG → base64 string."""
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
    _, buffer = cv2.imencode(".jpg", frame, encode_param)
    return base64.b64encode(buffer).decode("utf-8")


def build_message(frame: np.ndarray) -> dict:
    h, w = frame.shape[:2]
    return {
        "frame_id":  str(uuid.uuid4()),
        "timestamp": time.time(),
        "camera_id": CAMERA_ID,
        "width":     w,
        "height":    h,
        "frame_b64": frame_to_b64(frame),
    }


# ─── TCP Listener (tương thích tcp_example.py) ────────────────────────────────

class TcpFrameListener(threading.Thread):
    """
    Lắng nghe kết nối TCP từ client (giống tcp_example.py).
    Client gửi JSON payload chứa `frame_b64` qua newline-delimited protocol.
    """

    def __init__(self, host: str, port: int, producer, topic: str):
        super().__init__(daemon=True)
        self.host     = host
        self.port     = port
        self.producer = producer
        self.topic    = topic

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(5)
        logger.info("TCP listener đang chờ kết nối tại %s:%s", self.host, self.port)

        while True:
            conn, addr = srv.accept()
            logger.info("TCP client kết nối từ %s", addr)
            threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()

    def _handle_client(self, conn: socket.socket):
        buffer = b""
        MAX_BUFFER_SIZE = 50 * 1024 * 1024  # 50 MB limit
        with conn:
            while True:
                try:
                    if len(buffer) > MAX_BUFFER_SIZE:
                        logger.warning("TCP buffer exceeded limit, closing connection")
                        break
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buffer += chunk
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        self._process_tcp_payload(line)
                except (ConnectionResetError, OSError):
                    break

    def _process_tcp_payload(self, raw: bytes):
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            logger.warning("TCP: payload không hợp lệ")
            return

        # Nếu client gửi raw message (không có frame_b64), chỉ log
        if "frame_b64" not in payload:
            logger.info("TCP message (không có frame): %s", payload.get("message", ""))
            return

        # Bổ sung metadata nếu thiếu
        payload.setdefault("frame_id",  str(uuid.uuid4()))
        payload.setdefault("timestamp", time.time())
        payload.setdefault("camera_id", CAMERA_ID)

        if safe_send(self.producer, self.topic, payload):
            logger.info("TCP → Kafka | frame_id=%s", payload["frame_id"])


# ─── Camera Capture Loop ─────────────────────────────────────────────────────

def camera_capture_loop(producer):
    """
    Đọc frame từ camera và publish lên Kafka theo FRAME_RATE fps.
    Nếu CAMERA_SOURCE là số → webcam; nếu là chuỗi → RTSP/file.
    """
    src = int(CAMERA_SOURCE) if CAMERA_SOURCE.isdigit() else CAMERA_SOURCE
    cap = cv2.VideoCapture(src)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH,  FRAME_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)

    if not cap.isOpened():
        logger.error("Không thể mở camera source: %s", CAMERA_SOURCE)
        return

    logger.info("Camera %s đã mở, gửi %d fps lên topic '%s'", CAMERA_SOURCE, FRAME_RATE, TOPIC_FRAMES)
    interval = 1.0 / FRAME_RATE
    sent = 0

    while True:
        t0 = time.time()
        ret, frame = cap.read()
        if not ret:
            logger.warning("Không đọc được frame, thử lại...")
            time.sleep(0.5)
            continue

        msg = build_message(frame)
        if safe_send(producer, TOPIC_FRAMES, msg):
            sent += 1
            if sent % 50 == 0:
                logger.info("Đã gửi %d frames | frame_id=%s", sent, msg["frame_id"])

        elapsed = time.time() - t0
        time.sleep(max(0.0, interval - elapsed))


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    logger.info("Khởi động Camera Server...")
    producer = create_producer(KAFKA_BOOTSTRAP)

    # Khởi động TCP listener song song
    tcp_listener = TcpFrameListener(TCP_HOST, TCP_PORT, producer, TOPIC_FRAMES)
    tcp_listener.start()
    logger.info("TCP listener đã khởi động tại %s:%d", TCP_HOST, TCP_PORT)

    # Vòng lặp capture từ camera chạy trong thread riêng, để nếu camera
    # không khả dụng (ví dụ container không có webcam), TCP listener
    # vẫn tiếp tục hoạt động và tiến trình chính không bị thoát.
    camera_thread = threading.Thread(
        target=camera_capture_loop, args=(producer,), daemon=True
    )
    camera_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Camera Server dừng.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
