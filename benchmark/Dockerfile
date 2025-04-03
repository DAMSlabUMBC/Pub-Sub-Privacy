FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "Benchmark.py", "run", "benchmark-1.cfg", "127.0.0.1"]
