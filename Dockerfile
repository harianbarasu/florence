FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY florence ./florence

RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 florence \
    && chown -R florence:florence /app

USER florence

EXPOSE 8000

CMD ["florence"]
