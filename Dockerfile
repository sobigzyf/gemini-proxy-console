FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/chromium \
    UC_CLEAR_CACHE_ON_RETRY=0 \
    UC_DOWNLOAD_PROXY_ENABLED=0 \
    USE_XVFB=1 \
    XVFB_WHD=1920x1080x24

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    xvfb \
    fonts-noto-cjk \
    fonts-liberation \
    ca-certificates \
    tzdata \
    tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

EXPOSE 18423

ENTRYPOINT ["/usr/bin/tini", "--", "/bin/bash", "/app/docker/entrypoint.sh"]
