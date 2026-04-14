FROM ghcr.io/astral-sh/uv:0.11.6-python3.13-trixie@sha256:b3c543b6c4f23a5f2df22866bd7857e5d304b67a564f4feab6ac22044dde719b AS uv_source
FROM tianon/gosu:1.19-trixie@sha256:3b176695959c71e123eb390d427efc665eeb561b1540e82679c15e992006b8b9 AS gosu_source
FROM debian:13.4

ARG HERMES_PYTHON_EXTRAS=honcho
ARG INSTALL_NODE_RUNTIME=1
ARG INSTALL_BROWSER_TOOLS=1
ARG INSTALL_PLAYWRIGHT_BROWSERS=0
ARG INSTALL_WHATSAPP_BRIDGE=0

# Disable Python stdout buffering to ensure logs are printed immediately
ENV PYTHONUNBUFFERED=1 \
    NPM_CONFIG_UPDATE_NOTIFIER=false

# Store Playwright browsers outside the Hermes home mount so the build-time
# install survives any Railway/runtime volume overlay.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/hermes/.playwright

# Install system dependencies in one layer, clear APT cache.
# Florence's default production image stays lean; browser and WhatsApp
# tooling are opt-in via build args so hosted builders do not time out.
RUN set -eux; \
    packages="build-essential python3 ripgrep gcc python3-dev libffi-dev procps git"; \
    if [ "$INSTALL_NODE_RUNTIME" = "1" ] || [ "$INSTALL_BROWSER_TOOLS" = "1" ] || [ "$INSTALL_PLAYWRIGHT_BROWSERS" = "1" ] || [ "$INSTALL_WHATSAPP_BRIDGE" = "1" ]; then \
        packages="$packages nodejs npm"; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends $packages; \
    rm -rf /var/lib/apt/lists/*

# Non-root user for runtime; UID can be overridden via HERMES_UID at runtime
RUN useradd -u 10000 -m -d /opt/data hermes

COPY --chmod=0755 --from=gosu_source /gosu /usr/local/bin/
COPY --chmod=0755 --from=uv_source /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/

WORKDIR /opt/hermes

# Copy Node manifests first so browser-related dependency installs stay cached
# across normal Python/source edits.
COPY package.json package-lock.json /opt/hermes/
COPY scripts/whatsapp-bridge/package.json scripts/whatsapp-bridge/package-lock.json /opt/hermes/scripts/whatsapp-bridge/

# Install optional Node dependencies and Playwright as root. The default
# Florence deploy does not need these, so they are opt-in.
RUN set -eux; \
    if [ "$INSTALL_BROWSER_TOOLS" = "1" ] || [ "$INSTALL_PLAYWRIGHT_BROWSERS" = "1" ]; then \
        npm ci --prefer-offline --no-audit; \
    fi; \
    if [ "$INSTALL_PLAYWRIGHT_BROWSERS" = "1" ]; then \
        npx playwright install --with-deps chromium --only-shell; \
    fi; \
    if [ "$INSTALL_WHATSAPP_BRIDGE" = "1" ]; then \
        cd /opt/hermes/scripts/whatsapp-bridge; \
        npm install --prefer-offline --no-audit; \
    fi; \
    if command -v npm >/dev/null 2>&1; then \
        npm cache clean --force; \
    fi

COPY . /opt/hermes

# Hand ownership to hermes user, then install Python deps in a virtualenv
RUN chown -R hermes:hermes /opt/hermes
USER hermes

RUN uv venv && \
    if [ -n "$HERMES_PYTHON_EXTRAS" ]; then \
        uv pip install --no-cache-dir ".[${HERMES_PYTHON_EXTRAS}]"; \
    else \
        uv pip install --no-cache-dir .; \
    fi

USER root
RUN chmod +x /opt/hermes/docker/entrypoint.sh

ENV HERMES_HOME=/opt/data
RUN mkdir -p /opt/data
ENTRYPOINT [ "/opt/hermes/docker/entrypoint.sh" ]
