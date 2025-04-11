FROM python:3.10-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    iproute2 \
    iputils-ping \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "Benchmark.py", "run", "configs/examples/B1.cfg", "127.0.0.1"]
