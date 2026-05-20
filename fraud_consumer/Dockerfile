FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Consumer does not need to expose ports for inbound connections,
# but it does run a Prometheus exporter server on 8002.
EXPOSE 8002

CMD ["python", "main.py"]
