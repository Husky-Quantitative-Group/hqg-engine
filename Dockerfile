FROM python:3.11-slim

ENV http_proxy=http://user-proxy.business.uconn.edu:3128
ENV https_proxy=http://user-proxy.business.uconn.edu:3128

RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
EXPOSE 8000