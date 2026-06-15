"""
storage_server/storage_server.py
─────────────────────────────────────────────────────────────────────────────
Nhiệm vụ:
  1. Consume kết quả từ Kafka topic `detection-results`.
  2. Lưu số lượng người + bounding boxes vào PostgreSQL.
  3. Expose REST API đơn giản (FastAPI) để truy vấn thống kê.

Bảng DB:
  detection_events  – mỗi frame một bản ghi
  bounding_boxes    – mỗi bbox một bản ghi (FK → detection_events)
"""

import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import psycopg2.pool
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import sys
sys.path.insert(0, "/app")
from common.kafka_utils import create_consumer

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RESULTS   = os.getenv("KAFKA_TOPIC_RESULTS",  "detection-results")
GROUP_ID        = os.getenv("KAFKA_GROUP_ID",        "storage-group")
POSTGRES_DSN    = os.getenv(
    "POSTGRES_DSN",
    "postgresql://admin:secret@localhost:5432/person_counter",
)
API_PORT        = int(os.getenv("API_PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [StorageServer] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Connection pool
db_pool = None


# ─── Database ────────────────────────────────────────────────────────────────

def init_pool():
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        dsn=POSTGRES_DSN
    )
    logger.info("Database connection pool initialized")


def get_conn():
    return db_pool.getconn()


def put_conn(conn):
    db_pool.putconn(conn)


def wait_for_db(retries: int = 15, delay: float = 3.0):
    for i in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.close()
            logger.info("PostgreSQL đã sẵn sàng.")
            return
        except Exception as exc:
            logger.warning("Chờ PostgreSQL... (%d/%d): %s", i + 1, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Không kết nối được PostgreSQL sau nhiều lần thử.")


def save_detection(record: dict):
    """Lưu một kết quả phát hiện vào DB."""
    sql_event = """
        INSERT INTO detection_events
            (frame_id, camera_id, captured_at, processed_at, person_count, inference_time)
        VALUES (%s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s)
        ON CONFLICT (frame_id) DO NOTHING
        RETURNING id;
    """
    sql_bbox = """
        INSERT INTO bounding_boxes (event_id, x1, y1, x2, y2, confidence)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_event, (
                record["frame_id"],
                record["camera_id"],
                record["timestamp"],
                record.get("processed_at", record["timestamp"]),
                record["person_count"],
                record.get("inference_time", 0),
            ))
            row = cur.fetchone()
            if row:
                event_id = row[0]
                for bb in record.get("bounding_boxes", []):
                    cur.execute(sql_bbox, (
                        event_id,
                        bb["x1"], bb["y1"], bb["x2"], bb["y2"],
                        bb["confidence"],
                    ))
        conn.commit()
    finally:
        put_conn(conn)


# ─── Kafka Consumer Thread ───────────────────────────────────────────────────

def kafka_consumer_loop():
    consumer = create_consumer(KAFKA_BOOTSTRAP, TOPIC_RESULTS, GROUP_ID, "earliest")
    logger.info("Storage consumer đang lắng nghe topic '%s'...", TOPIC_RESULTS)
    saved = 0
    for msg in consumer:
        record = msg.value
        try:
            save_detection(record)
            saved += 1
            if saved % 20 == 0:
                logger.info(
                    "Đã lưu %d bản ghi | frame_id=%s | người=%d",
                    saved, record.get("frame_id"), record.get("person_count"),
                )
        except Exception as exc:
            logger.error("Lỗi khi lưu frame_id=%s: %s", record.get("frame_id"), exc)


# ─── FastAPI ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    wait_for_db()
    init_pool()
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()
    yield
    if db_pool:
        db_pool.closeall()


app = FastAPI(title="Person Counter – Storage API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
def health():
    return {"status": "ok", "service": "storage-server"}


@app.get("/stats/summary")
def summary(camera_id: str = Query(None, description="Lọc theo camera")):
    """Thống kê tổng hợp: tổng frame, tổng người, trung bình người/frame."""
    where = "WHERE camera_id = %s" if camera_id else ""
    params = (camera_id,) if camera_id else ()
    sql = f"""
        SELECT
            COUNT(*)                        AS total_frames,
            COALESCE(SUM(person_count), 0)  AS total_persons_detected,
            ROUND(AVG(person_count)::numeric, 2) AS avg_persons_per_frame,
            MAX(person_count)               AS max_persons,
            MAX(captured_at)                AS last_seen
        FROM detection_events {where};
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return dict(cur.fetchone())
    finally:
        put_conn(conn)


@app.get("/stats/timeseries")
def timeseries(
    camera_id: str = Query(None),
    interval:  str = Query("minute", description="minute | hour | day"),
    limit:     int = Query(60),
):
    """Chuỗi thời gian số lượng người theo khoảng thời gian."""
    # Whitelist intervals to prevent SQL injection
    valid_intervals = {"minute", "hour", "day"}
    if interval not in valid_intervals:
        raise HTTPException(status_code=400, detail=f"Invalid interval. Use: {valid_intervals}")
    
    where  = "WHERE camera_id = %s" if camera_id else ""
    params = [camera_id] if camera_id else []
    params += [limit]
    sql = f"""
        SELECT
            date_trunc('{interval}', captured_at) AS bucket,
            ROUND(AVG(person_count)::numeric, 2) AS avg_persons,
            MAX(person_count)                    AS max_persons,
            COUNT(*)                             AS frame_count
        FROM detection_events {where}
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT %s;
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        put_conn(conn)


@app.get("/events/recent")
def recent_events(limit: int = Query(20), camera_id: str = Query(None)):
    """Lấy các sự kiện phát hiện gần nhất."""
    where  = "WHERE camera_id = %s" if camera_id else ""
    params = [camera_id] if camera_id else []
    params.append(limit)
    sql = f"""
        SELECT frame_id, camera_id, captured_at, person_count, inference_time
        FROM detection_events {where}
        ORDER BY captured_at DESC LIMIT %s;
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        put_conn(conn)


@app.get("/events/{frame_id}/boxes")
def frame_boxes(frame_id: str):
    """Lấy tất cả bounding boxes của một frame cụ thể."""
    sql = """
        SELECT bb.x1, bb.y1, bb.x2, bb.y2, bb.confidence
        FROM bounding_boxes bb
        JOIN detection_events de ON bb.event_id = de.id
        WHERE de.frame_id = %s;
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (frame_id,))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="frame_id không tồn tại")
        return [dict(r) for r in rows]
    finally:
        put_conn(conn)


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("storage_server:app", host="0.0.0.0", port=API_PORT, reload=False)
