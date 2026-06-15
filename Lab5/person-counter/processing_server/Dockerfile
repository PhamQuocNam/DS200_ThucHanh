FROM python:3.11-slim

WORKDIR /app

# OpenCV / Torch runtime libs
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processing_server.py .

# Bake the model weights into the image
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

CMD ["python", "processing_server.py"]
