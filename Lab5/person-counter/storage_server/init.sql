-- init.sql – Khởi tạo schema cho hệ thống đếm người

-- Extension TimescaleDB (nếu dùng TimescaleDB image)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS detection_events (
    id             BIGSERIAL PRIMARY KEY,
    frame_id       TEXT UNIQUE NOT NULL,
    camera_id      TEXT NOT NULL DEFAULT 'cam-01',
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at   TIMESTAMPTZ,
    person_count   INTEGER NOT NULL DEFAULT 0,
    inference_time FLOAT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bounding_boxes (
    id          BIGSERIAL PRIMARY KEY,
    event_id    BIGINT REFERENCES detection_events(id) ON DELETE CASCADE,
    x1          INTEGER NOT NULL,
    y1          INTEGER NOT NULL,
    x2          INTEGER NOT NULL,
    y2          INTEGER NOT NULL,
    confidence  FLOAT NOT NULL
);

-- Index để tăng tốc truy vấn theo thời gian & camera
CREATE INDEX IF NOT EXISTS idx_events_camera_time
    ON detection_events (camera_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_boxes_event
    ON bounding_boxes (event_id);

-- View tiện lợi: thống kê theo phút
CREATE OR REPLACE VIEW person_count_by_minute AS
SELECT
    date_trunc('minute', captured_at) AS minute,
    camera_id,
    ROUND(AVG(person_count)::numeric, 2) AS avg_persons,
    MAX(person_count)                    AS max_persons,
    COUNT(*)                             AS frame_count
FROM detection_events
GROUP BY 1, 2
ORDER BY 1 DESC;
