FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Persistent data lives in a volume mounted at /app/data
VOLUME ["/app/data"]

EXPOSE 8000

CMD ["python", "run.py"]
