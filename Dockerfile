FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
# gcc/musl-dev often needed for some python packages like uvicorn/httptools/uvloop
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (will be overridden by docker-compose)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
