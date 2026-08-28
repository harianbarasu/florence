Label: wayfinder:task
Type: task
Status: open
Blocked by: 03, 13, 17

# Port progressive capability discovery

## Question

Once Florence's catalog is large enough to justify it, how can the model discover and activate only the capabilities already authorized for the current adult, surface, account, and request without placing every schema in every prompt or letting discovery bypass policy?

Port Hermes's discovery algorithm and normalized descriptions from `tools/registry.py`, `toolsets.py`, and `tools/tool_search.py`, plus Pi's additive dynamic-tool activation example and active-only prompt metadata. Keep the policy-filtered catalog inside the chosen Florence capability module and exclude coding/shell/filesystem tools before discovery.
