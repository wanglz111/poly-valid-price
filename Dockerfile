FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_chainlink_reports.py /app/
COPY fetch_polymarket_prices.py /app/

# Default entrypoint can be overridden by docker run command
ENTRYPOINT ["python", "/app/fetch_polymarket_prices.py"]
