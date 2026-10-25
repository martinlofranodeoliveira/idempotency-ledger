"""Run from a source checkout: PYTHONPATH=. python examples/synthetic_demo.py."""

import hashlib
import tempfile
from pathlib import Path

from idempotency_ledger import Ledger


def main():
    fingerprint = hashlib.sha256(b'{"synthetic_invoice":42}').hexdigest()
    with tempfile.TemporaryDirectory(prefix="synthetic-ledger-demo-") as directory:
        ledger = Ledger.create(Path(directory) / "ledger.sqlite3")
        first = ledger.claim("invoice:42", fingerprint, now_ms=1000, lease_ms=500)
        print("first:", first["status"])
        print("duplicate:", ledger.claim("invoice:42", fingerprint, now_ms=1001, lease_ms=500))
        print("completed:", ledger.complete("invoice:42", first["token"],
                                             {"invoice": 42, "synthetic": True}, now_ms=1002))
        print("retry:", ledger.claim("invoice:42", fingerprint, now_ms=9999, lease_ms=500))
        print("temporary database removed on exit")


if __name__ == "__main__":
    main()
