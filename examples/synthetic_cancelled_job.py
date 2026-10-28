"""Return an unstarted synthetic task, then let another worker reclaim it."""
import hashlib
import json
from pathlib import Path
import tempfile

from idempotency_ledger import Ledger


def main():
    fingerprint = hashlib.sha256(b"synthetic cancelled task").hexdigest()
    with tempfile.TemporaryDirectory(prefix="synthetic-cancellation-") as root:
        ledger = Ledger.create(Path(root) / "ledger.sqlite3")
        first = ledger.claim("task:1", fingerprint, now_ms=100, lease_ms=5000)
        # Worker one chooses to stop BEFORE doing any work or external effect.
        assert ledger.abandon("task:1", first["token"], now_ms=110)
        second = ledger.claim("task:1", fingerprint, now_ms=110, lease_ms=1000)
        assert second["status"] == "claimed" and second["token"] != first["token"]
        assert not ledger.complete("task:1", first["token"], "stale", now_ms=111)
        assert ledger.complete("task:1", second["token"], {"synthetic": "done"}, now_ms=111)
        print(json.dumps(ledger.claim("task:1", fingerprint, now_ms=2000, lease_ms=1)))


if __name__ == "__main__":
    main()
