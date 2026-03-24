FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HERMES_HOME=/data/hermes-home \
    FLORENCE_HTTP_HOST=0.0.0.0

WORKDIR /app

RUN mkdir -p /data/hermes-home

COPY . /app

RUN pip install --upgrade pip && pip install .

EXPOSE 8080

CMD ["florence-server"]
