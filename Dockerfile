FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Fail the image build if the runtime dependency lock is incomplete.
RUN python -c "from app.main import app; assert app.title == 'Multi-Agent Backend API'"

# The SQLite database and the object store live here; mount a volume to keep them.
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --start-interval=1s --retries=3 \
    CMD python -c "import urllib.request; response = urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3); raise SystemExit(0 if response.status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
