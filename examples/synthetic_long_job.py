"""Run from a checkout: PYTHONPATH=. python examples/synthetic_long_job.py.

All timestamps and work are synthetic; no clock, sleep, or external side effects.
"""

import hashlib
import tempfile
from pathlib import Path

from idempotency_ledger import Ledger


def main():
    fingerprint = hashlib.sha256(b'{"synthetic_batch":42,"parts":3}').hexdigest()
    with tempfile.TemporaryDirectory(prefix="synthetic-long-job-") as directory:
        ledger = Ledger.create(Path(directory) / "ledger.sqlite3")
        claim = ledger.claim("batch:42", fingerprint, now_ms=1000, lease_ms=500)
        if claim["status"] != "claimed":
            print("claim unavailable:", claim["status"])
            return

        token = claim["token"]
        parts = []
        for part, now_ms in enumerate((1400, 1800, 2200), start=1):
            # The application explicitly renews between bounded pieces of work.
            # Real work needs its own clock policy and downstream idempotency.
            if not ledger.renew("batch:42", token, now_ms=now_ms, lease_ms=500):
                print("ownership lost; stop starting work and discard this result")
                return
            parts.append(part * part)  # Synthetic work only.
            print("renewed before part:", part)

        accepted = ledger.complete("batch:42", token, {"parts": parts}, now_ms=2300)
        print("completed:", accepted)
        print("retry:", ledger.claim("batch:42", fingerprint, now_ms=9999, lease_ms=500))
        print("temporary database removed on exit")


if __name__ == "__main__":
    main()
