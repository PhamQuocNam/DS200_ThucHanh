# 🎯 Hệ thống Đếm Người Qua Camera – Kafka Architecture

<video controls src="demo.mp4" title="Demo"></video>

## Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────┐
│                         HỆ THỐNG ĐẾM NGƯỜI                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  [Camera / TCP Client]                                              │
│         │                                                           │
│         ▼                                                           │
│  ┌──────────────┐    Kafka Topic      ┌─────────────────────┐      │
│  │ CAMERA       │  ──"raw-frames"──▶  │  PROCESSING         │      │
│  │ SERVER       │                     │  SERVER             │      │
│  │              │                     │  (YOLOv8 Detection) │      │
│  │ • Webcam     │                     │                     │      │
│  │ • RTSP       │                     │  • Decode frame     │      │
│  │ • TCP input  │                     │  • Detect persons   │      │
│  │              │                     │  • Output BBoxes    │      │
│  └──────────────┘                     └──────────┬──────────┘      │
│                                                  │                 │
│                                     Kafka Topic  │                 │
│                                  "detection-results"               │
│                                                  │                 │
│                                                  ▼                 │
│                                       ┌──────────────────┐        │
│                                       │  STORAGE         │        │
│                                       │  SERVER          │        │
│                                       │                  │        │
│                                       │  • PostgreSQL    │        │
│                                       │  • REST API      │        │
│                                       └──────────────────┘        │
│                                                                     │
│  ──────── Kafka Broker (port 9092) ──────── Kafka UI (port 8080)  │
└─────────────────────────────────────────────────────────────────────┘
```

## Kafka Topics

| Topic               | Producer        | Consumer            | Nội dung                       |
|---------------------|-----------------|---------------------|--------------------------------|
| `raw-frames`        | Camera Server   | Processing Server   | Frame ảnh JPEG (base64 + meta) |
| `detection-results` | Processing Server | Storage Server    | BBoxes + số người + metadata   |

## Cấu trúc thư mục

```
person-counter/
├── docker-compose.yml          # Toàn bộ hạ tầng
├── tcp_frame_client.py         # Client TCP (tương thích tcp_example.py)
├── common/
│   └── kafka_utils.py          # Tiện ích Kafka dùng chung
├── camera_server/
│   ├── camera_server.py        # Server nhận frame & publish lên Kafka
│   ├── Dockerfile
│   └── requirements.txt
├── processing_server/
│   ├── processing_server.py    # Server YOLOv8 – phát hiện người
│   ├── Dockerfile
│   └── requirements.txt
└── storage_server/
    ├── storage_server.py       # Server lưu trữ + REST API
    ├── init.sql                # Schema PostgreSQL
    ├── Dockerfile
    └── requirements.txt
```

## Hướng dẫn khởi động

### Yêu cầu
- Docker & Docker Compose v2+
- RAM ≥ 4 GB (YOLOv8 cần ~1 GB)

### Bước 1 – Khởi động toàn bộ hệ thống

```bash
docker compose up --build -d
```

### Bước 2 – Kiểm tra trạng thái

```bash
docker compose ps
docker compose logs -f processing-server
```

### Bước 3 – Giao diện Kafka UI

Mở trình duyệt: **http://localhost:8080**

### Bước 4 – REST API thống kê

```bash
# Tóm tắt tổng hợp
curl http://localhost:8000/stats/summary

# Chuỗi thời gian (mỗi phút)
curl "http://localhost:8000/stats/timeseries?interval=1+minute&limit=30"

# Sự kiện gần nhất
curl http://localhost:8000/events/recent?limit=10
```

## Cấu hình biến môi trường

### Camera Server

| Biến                      | Mặc định     | Mô tả                              |
|---------------------------|--------------|------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Địa chỉ Kafka broker             |
| `KAFKA_TOPIC_FRAMES`      | raw-frames   | Tên topic gửi frame                |
| `CAMERA_SOURCE`           | 0            | 0 = webcam, URL = RTSP/file        |
| `FRAME_RATE`              | 5            | Số frame/giây gửi lên Kafka        |
| `TCP_PORT`                | 6100         | Cổng TCP nhận frame từ client      |

### Processing Server

| Biến                      | Mặc định           | Mô tả                              |
|---------------------------|--------------------|------------------------------------|
| `KAFKA_TOPIC_FRAMES`      | raw-frames         | Topic đọc frame vào                |
| `KAFKA_TOPIC_RESULTS`     | detection-results  | Topic ghi kết quả ra               |
| `MODEL_CONFIDENCE`        | 0.5                | Ngưỡng tin cậy YOLOv8              |
| `YOLO_MODEL`              | yolov8n.pt         | Phiên bản model (n/s/m/l/x)        |

### Storage Server

| Biến                      | Mặc định                                          | Mô tả         |
|---------------------------|---------------------------------------------------|---------------|
| `KAFKA_TOPIC_RESULTS`     | detection-results                                 | Topic đọc vào |
| `POSTGRES_DSN`            | postgresql://admin:secret@postgres:5432/person_counter | Kết nối DB |
| `API_PORT`                | 8000                                              | Cổng REST API |

## Sử dụng TCP Client (tương thích tcp_example.py)

```bash
# Gửi frame từ webcam qua TCP
python tcp_frame_client.py --source 0 --host localhost --port 6100 --fps 5

# Gửi frame từ file video
python tcp_frame_client.py --source video.mp4 --fps 10

# Gửi frame từ RTSP
python tcp_frame_client.py --source "rtsp://192.168.1.100/stream" --fps 5
```

## Schema Kafka Messages

### Topic `raw-frames`
```json
{
  "frame_id":  "uuid-string",
  "timestamp": 1718000000.123,
  "camera_id": "cam-01",
  "width":     640,
  "height":    480,
  "frame_b64": "<base64-encoded JPEG>"
}
```

### Topic `detection-results`
```json
{
  "frame_id":       "uuid-string",
  "timestamp":      1718000000.123,
  "camera_id":      "cam-01",
  "processed_at":   1718000000.456,
  "inference_time": 0.045,
  "person_count":   3,
  "bounding_boxes": [
    {"x1": 120, "y1": 80, "x2": 200, "y2": 350, "confidence": 0.92, "label": "person"},
    {"x1": 300, "y1": 60, "x2": 390, "y2": 340, "confidence": 0.87, "label": "person"}
  ]
}
```

## Mở rộng hệ thống

- **Scale Processing Server**: `docker compose up --scale processing-server=3`
- **Thêm camera**: Tăng `CAMERA_ID` và chạy nhiều camera-server với cổng TCP khác nhau
- **GPU acceleration**: Đổi base image sang `nvidia/cuda` và thêm `device=cuda` trong YOLO
- **Kafka Partitions**: Tăng số partition của `raw-frames` để xử lý song song nhiều consumer
