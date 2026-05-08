# Hermes Upstream Tracking

Florence tracks `NousResearch/hermes-agent` as an external upstream tree. Florence does not assume it can merge into or publish changes to Hermes.

The goal is to keep Florence product behavior under `florence/` while carrying only a small, documented local Hermes compatibility patch layer.

## Remotes

- `upstream`: `git@github.com:NousResearch/hermes-agent.git`
- `origin`: Florence product repository

## Pull Workflow

1. Refresh the vendored upstream snapshot:

   ```bash
   source .venv/bin/activate
   python scripts/update_vendored_hermes.py
   ```

2. Create a backup branch before reconciling:

   ```bash
   git branch backup/pre-hermes-upstream-$(date +%Y%m%d)
   ```

3. Review the patch ledger:

   ```bash
   sed -n '1,140p' PATCHES.md
   ```

4. Reconcile any remaining root-level Hermes files or local patches with the new vendored snapshot.

5. Reconcile only the items listed in `PATCHES.md`.

6. Keep Florence product changes under `florence/`, `web/`, deployment files, docs, or tests.

7. Verify the vendored snapshot marker matches the requested upstream ref:

   ```bash
   source .venv/bin/activate
   python scripts/update_vendored_hermes.py --check --no-fetch
   ```

8. Run at minimum through the repo test wrapper:

   ```bash
   scripts/run_tests.sh \
     tests/florence/test_hermes_boundary.py \
     tests/scripts/test_update_vendored_hermes.py \
     tests/test_model_tools.py \
     tests/test_toolsets.py \
     tests/tools/test_florence_household_tool.py
   ```

## Vendored Hermes

Florence keeps a clean upstream snapshot under:

```text
vendor/hermes-agent/
```

This tree is generated from `upstream/main` and should not contain Florence product code. Refresh it with:

```bash
source .venv/bin/activate
python scripts/update_vendored_hermes.py
```

The script records the exported source ref in `vendor/hermes-agent/.florence-vendor-source`.
Use `python scripts/update_vendored_hermes.py --check --no-fetch` after
reconciling to verify the marker and snapshot still match the selected upstream
ref without rewriting the vendor tree.

Runtime resolution goes through:

```text
florence/agent/hermes_runtime.py
```

By default Florence still uses the historical root-level Hermes modules during the transition. To test or deploy the vendored path explicitly:

```bash
export FLORENCE_USE_VENDORED_HERMES=1
export FLORENCE_HERMES_VENDOR_PATH=$PWD/vendor/hermes-agent
```

The migration target is that Florence product code imports Hermes only through `florence.agent`, while the root-level Hermes files become replaceable by the vendored/tracked tree.

## Rules

- Do not add normal family-product behavior to Hermes core.
- If a local Hermes patch is needed, add or update the entry in `PATCHES.md`.
- If upstream Hermes independently adds an equivalent hook, delete the local patch.
- Do not prepare upstream PR branches unless we explicitly decide to contribute a generic fix.
- Do not let `toolsets.py` accumulate Florence product toolsets again; use `florence/agent/tool_registry.py`.
- Do not move Florence household tools back into root `tools/`; keep them product-owned under `florence/tools/`.
- Do not edit `vendor/hermes-agent/` directly for Florence product behavior; refresh it from upstream and reconcile local patches outside the vendor tree.

## Current Local Patch Categories

- Agent construction/runtime knobs used by product runtimes.
- Scoped memory/session plumbing.
- Toolset filtering semantics.
- Deployment/app-surface divergence.
