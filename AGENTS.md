# Florence Agent Guide

This repository is intentionally small. Preserve the service boundary:

1. Linq transport code belongs in `florence/linq.py`.
2. Hermes-specific code belongs behind `florence/hermes.py`.
3. Time parsing and reminder normalization belong in `florence/timekeeper.py`.
4. Email/calendar/source filtering belongs in `florence/policy.py`.
5. User-facing tone belongs in `florence/tone.py`.

Do not reintroduce a large fork of Hermes Agent into this repository. If Florence
needs more agentic capability, extend the adapter or run Hermes Agent as an
external dependency.

Run tests with:

```bash
pytest
```
