FROM python:3.12-slim

ARG INSTALL_HERMES_AGENT=0
ARG HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git
ARG HERMES_AGENT_REF=

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY florence ./florence

RUN set -eux; \
    pip install --no-cache-dir .; \
    if [ "$INSTALL_HERMES_AGENT" = "1" ]; then \
        if ! printf '%s' "$HERMES_AGENT_REF" | grep -Eq '^([0-9a-fA-F]{40}|[0-9a-fA-F]{64})$'; then \
            echo "HERMES_AGENT_REF must be a full pinned Git commit SHA when INSTALL_HERMES_AGENT=1" >&2; \
            exit 1; \
        fi; \
        apt-get update; \
        apt-get install -y --no-install-recommends git; \
        rm -rf /var/lib/apt/lists/*; \
        git clone --filter=blob:none "$HERMES_AGENT_REPO" /opt/hermes-agent; \
        git -C /opt/hermes-agent fetch --depth 1 origin "$HERMES_AGENT_REF"; \
        git -C /opt/hermes-agent checkout --detach FETCH_HEAD; \
        git -C /opt/hermes-agent rev-parse HEAD > /opt/hermes-agent/.florence-hermes-ref; \
        pip install --no-cache-dir /opt/hermes-agent; \
    fi; \
    useradd --create-home --uid 10001 florence; \
    chown -R florence:florence /app; \
    if [ -d /opt/hermes-agent ]; then \
        chown -R florence:florence /opt/hermes-agent; \
    fi

USER florence

EXPOSE 8000

CMD ["sh", "-c", "if [ \"${FLORENCE_PROCESS:-web}\" = \"worker\" ]; then exec florence-worker; else exec florence; fi"]
