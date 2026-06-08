from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dockerfile_has_pinned_hermes_install_hook():
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "ARG INSTALL_HERMES_AGENT=0" in dockerfile
    assert "ARG HERMES_AGENT_REPO=https://github.com/NousResearch/hermes-agent.git" in dockerfile
    assert "ARG HERMES_AGENT_REF=" in dockerfile
    assert "HERMES_AGENT_REF must be a full pinned Git commit SHA" in dockerfile
    assert "grep -Eq '^([0-9a-fA-F]{40}|[0-9a-fA-F]{64})$'" in dockerfile
    assert 'git clone --filter=blob:none "$HERMES_AGENT_REPO" /opt/hermes-agent' in dockerfile
    assert 'git -C /opt/hermes-agent fetch --depth 1 origin "$HERMES_AGENT_REF"' in dockerfile
    assert "git -C /opt/hermes-agent checkout --detach FETCH_HEAD" in dockerfile
    assert (
        "git -C /opt/hermes-agent rev-parse HEAD > /opt/hermes-agent/.florence-hermes-ref"
        in dockerfile
    )
    assert "pip install --no-cache-dir /opt/hermes-agent" in dockerfile


def test_dockerfile_can_start_web_or_worker_from_same_image():
    dockerfile = (ROOT / "Dockerfile").read_text()

    assert "FLORENCE_PROCESS:-web" in dockerfile
    assert "exec florence-worker" in dockerfile
    assert "exec florence" in dockerfile


def test_env_example_points_docker_hermes_path_at_image_checkout():
    env_example = (ROOT / ".env.example").read_text()

    assert "INSTALL_HERMES_AGENT=1" in env_example
    assert "HERMES_AGENT_REF=" in env_example
    assert "FLORENCE_HERMES_AGENT_PATH=/opt/hermes-agent" in env_example
    assert "FLORENCE_HERMES_RUNTIME_HOME=/tmp/florence-hermes-home" in env_example
    assert "FLORENCE_HERMES_STRICT=1" in env_example


def test_readme_warns_to_pin_hermes_before_container_build():
    readme = (ROOT / "README.md").read_text()

    assert "set HERMES_AGENT_REF to a full pinned Hermes commit SHA" in readme
    assert "build intentionally fails while INSTALL_HERMES_AGENT=1 and HERMES_AGENT_REF is" in readme
    assert "pilot image cannot bake a floating Hermes checkout" in readme


def test_compose_exposes_hermes_build_args_and_runtime_env_to_app_and_worker():
    compose = (ROOT / "docker-compose.yml").read_text()

    assert "INSTALL_HERMES_AGENT: ${INSTALL_HERMES_AGENT:-0}" in compose
    assert "HERMES_AGENT_REPO: ${HERMES_AGENT_REPO:-https://github.com/NousResearch/hermes-agent.git}" in compose
    assert "HERMES_AGENT_REF: ${HERMES_AGENT_REF:-}" in compose
    assert compose.count("<<: *florence-runtime-env") == 2
    for env_name in (
        "FLORENCE_DATABASE_URL",
        "FLORENCE_ADMIN_API_KEY",
        "LINQ_API_KEY",
        "LINQ_WEBHOOK_SECRET",
        "GOOGLE_CLIENT_ID",
        "GOOGLE_CLIENT_SECRET",
        "HERMES_AGENT_REF",
        "FLORENCE_HERMES_AGENT_PATH",
        "FLORENCE_HERMES_PROVIDER",
        "FLORENCE_HERMES_MODEL",
        "FLORENCE_HERMES_TOOLSETS",
        "FLORENCE_HERMES_RUNTIME_HOME",
        "FLORENCE_HERMES_STRICT",
    ):
        assert f"{env_name}: ${{{env_name}" in compose
