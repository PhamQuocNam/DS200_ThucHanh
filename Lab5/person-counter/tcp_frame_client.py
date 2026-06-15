"""
tcp_frame_client.py
─────────────────────────────────────────────────────────────────────────────
Script client tương thích với tcp_example.py gốc.
Gửi khung hình từ webcam / file / RTSP đến Camera Server qua TCP.

Sử dụng:
  python tcp_frame_client.py --source 0 --host localhost --port 6100 --fps 5
"""

import argparse
import base64
import json
import socket
import time
import uuid

import cv2


def frame_to_b64(frame, quality: int = 75) -> str:
    _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    return base64.b64encode(buf).decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="TCP Frame Client")
    parser.add_argument("--source", default="0",   help="Nguồn camera: số (0,1) hoặc URL RTSP")
    parser.add_argument("--host",   default="localhost")
    parser.add_argument("--port",   type=int, default=6100)
    parser.add_argument("--fps",    type=int, default=5,   help="Số frame/giây gửi đến server")
    parser.add_argument("--width",  type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--camera-id", default="cam-tcp-01")
    args = parser.parse_args()

    src = int(args.source) if args.source.isdigit() else args.source
    cap = cv2.VideoCapture(src)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH,  args.width)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, args.height)

    # ── Kết nối TCP (giống tcp_example.py) ──────────────────────────────────
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.connect((args.host, args.port))
    print(f"Đã kết nối đến {args.host}:{args.port}")

    interval = 1.0 / args.fps
    sent = 0

    try:
        while True:
            t0 = time.time()
            ret, frame = cap.read()
            if not ret:
                print("Không đọc được frame.")
                time.sleep(0.5)
                continue

            h, w = frame.shape[:2]
            payload = {
                "frame_id":  str(uuid.uuid4()),
                "timestamp": time.time(),
                "camera_id": args.camera_id,
                "width":     w,
                "height":    h,
                "frame_b64": frame_to_b64(frame),
            }

            data = (json.dumps(payload) + "\n").encode("utf-8")
            try:
                s.sendall(data)
                sent += 1
                if sent % 20 == 0:
                    print(f"Đã gửi {sent} frames")
            except (BrokenPipeError, OSError) as exc:
                print(f"Lỗi kết nối TCP: {exc}")
                break

            elapsed = time.time() - t0
            time.sleep(max(0.0, interval - elapsed))

    except KeyboardInterrupt:
        print("\nDừng client.")
    finally:
        cap.release()
        s.close()


if __name__ == "__main__":
    main()
