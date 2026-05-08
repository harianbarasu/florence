# Florence Fork Boundary

This file tracks intentional Florence changes outside the Florence product package. The long-term goal is to drive this list toward zero by moving family-specific behavior under `florence/`.

## Boundary Rules

- Hermes owns the generic agent runtime: model loop, tool registry, providers, sessions, memory, MCP, delegation, and generic gateway behavior.
- Florence owns family product behavior: household identity, group/private scope, household state, Google grounding, transport adapters, and Florence tools.
- Do not add new Florence product behavior to Hermes core files. Add it under `florence/` and expose it through a generic Hermes extension point when needed.

## Current Intentional Patches

### Local Hermes compatibility patches to preserve while tracking upstream

- `run_agent.py`
  - `disabled_toolsets` support through agent construction and CLI paths.
  - caller-provided Honcho session plumbing: `honcho_session_key`, `honcho_manager`, and related config.
  - `persist_session=False` support for callers that need non-persistent internal turns.
  - Codex Responses prompt-cache-key normalization for provider compatibility.
- `model_tools.py`
  - `disabled_toolsets` are applied after explicit `enabled_toolsets`, so product runtimes can narrow a toolset and still remove unsafe categories.
- `tools/delegate_tool.py`
  - child agents inherit blocked/disabled toolset constraints, including the Florence need to keep messaging out of delegated child runs.
- `tools/session_search_tool.py`
  - scoped session search support through allowed session ids.
- `plugins/memory/honcho/__init__.py`
  - external Honcho manager/session-key support so product runtimes can map memory to household/channel scope.

### Florence product integration boundaries

- `toolsets.py`
  - no static Florence toolsets should live here.
  - Florence registers `florence_chat`, `florence_onboarding`, and `florence_briefing` from `florence/agent/tool_registry.py`.

### Deployment and app-surface divergence

- `vendor/hermes-agent/`
  - clean upstream Hermes snapshot used to make the dependency boundary explicit.
  - do not put Florence product code here.
  - refresh from `upstream/main` and reconcile local compatibility patches in the Florence layer.
- `Dockerfile`
  - Florence production image defaults to `python3 -m florence.server` and keeps browser/Node build work opt-in.
- `docker/entrypoint.sh`
  - Florence deploy defaults to the Florence server while preserving Hermes command passthrough behavior.
- `pyproject.toml`
  - includes Florence packages and `florence-server` / `florence-worker` entry points.
- `requirements.txt`
  - Florence deployment dependency pin surface.
- `.dockerignore`, `.env.example`, `railway.json`, `docs/florence-railway.md`, `scripts/reset_florence_state.py`
  - Florence deployment and operational support.
- `web/`
  - Florence product UI/control plane, intentionally not upstream Hermes' generic dashboard.

## Exit Criteria

The fork boundary is healthy when:

- Hermes core can import and run without importing `florence`.
- Florence tools are product-owned and registered through a generic extension point.
- Updating Hermes is a documented upstream pull plus PATCHES.md reconciliation, not a broad product-code merge conflict.
- This file contains only deployment/app-surface differences and a short list of local generic runtime hooks.
