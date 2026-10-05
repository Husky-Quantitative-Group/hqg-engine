"""Manual mock-stack replacement, after stopping the old provider and waiting."""

import os
import sys
import time
from uuid import uuid4

sys.path.insert(0, "/dashboard/local")
from server import LocalStore

store = LocalStore(os.environ["MOCK_DASHBOARD_DB"])
account_id = os.environ["HQG_ACCOUNT_ID"]
state = store.get(account_id, "state")
if not state or time.time() - (state.get("last_sync") or 0) < 15:
    raise SystemExit(
        "Stop the previous provider and wait at least 15 seconds after its last sync"
    )

state["boot_id"] = None
state["observed"] = None
state["last_sync"] = None
state["control_version"] += 1

for mode in state["modes"].values():
    mode.update(
        desired_state="paused",
        resume_incident_id=None,
        stop_version=state["control_version"],
    )

old_generation = state["generation"]
state["generation"] += 1

store.commit(
    account_id,
    old_generation,
    state,
    {
        "audit#"
        + str(uuid4()): {
            "actor": "local-maintainer",
            "kind": "authorize_replacement_boot",
            "control_version": state["control_version"],
            "timestamp": time.time(),
        }
    },
)

print("Replacement boot authorized. Configuration retained. Both modes require Resume.")
