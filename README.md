# idempotency-ledger

A small Python library for coordinating retries through a local SQLite ledger.
It gives one caller a temporary claim, fences stale completions, and permanently
replays the JSON result of a completed claim. Python 3.10+; no runtime dependencies.

[Documentação em português](README.pt-BR.md)

Useful for a small local worker, a single-host batch import, or a script that
receives the same request more than once. The library coordinates ledger state;
your application performs the work and chooses what its request fingerprint means.

## Install and try

From a checkout:

```sh
python -m pip install .
python -B -m unittest discover -s tests -v
PYTHONPATH=. python -B examples/synthetic_demo.py
PYTHONPATH=. python -B examples/synthetic_long_job.py
```

The demo uses synthetic data in a temporary directory and removes its database
on exit. Installation needs a build backend (`setuptools`); runtime and tests use
only Python's standard library.

## API example

```python
import hashlib
from pathlib import Path
from idempotency_ledger import Ledger, LedgerError

# Create once at a dedicated, previously unused path. Parent must exist.
ledger = Ledger.create(Path("/tmp/my-dedicated-ledger.sqlite3"))
# In later invocations, open it with Ledger(path); this never initializes a DB.

# Your application defines a stable canonical request representation.
fingerprint = hashlib.sha256(b'{"invoice":42,"version":1}').hexdigest()
claim = ledger.claim("invoice:42", fingerprint, now_ms=1000, lease_ms=5000)

if claim["status"] == "claimed":
    # Do your work here, with your own downstream idempotency/fencing strategy.
    result = {"invoice": 42, "state": "prepared"}  # Synthetic example only.
    accepted = ledger.complete("invoice:42", claim["token"], result, now_ms=1001)
    if not accepted:
        print("Ownership expired or changed; this result was not stored.")
elif claim["status"] == "replay":
    print(claim["result"])
elif claim["status"] == "busy":
    print("An owner holds the claim until", claim["expires_at"])
elif claim["status"] == "conflict":
    print("This key belongs to a different request fingerprint.")
```

The library never reads a clock. `now_ms` is an explicit application-provided
integer, expressed in a consistent time domain across all callers of the ledger.
Use a reliable shared clock policy in your application; the example's timestamps
are synthetic. `expires_at` uses that same time domain. The token is an opaque
ownership token, not a monotonic sequence suitable for downstream ordering.

## Contract

`Ledger.create(path) -> Ledger` atomically publishes a new, identified SQLite
database with permissions `0600`. It stages in the same parent directory, then
uses a hard link without overwriting any existing entry, including a broken
symbolic link. It cleans up its staging file on success and ordinary failures.
The filesystem must support hard links. This is atomic publication, not a promise
that a new directory entry survives every possible power-loss scenario.

`Ledger(path)` opens an existing recognized ledger. Missing paths, ordinary
SQLite databases, unknown versions, and unrecognized schemas raise `LedgerError`.
Read-only identification does not initialize or recover another SQLite database.
Each operation checks the identity again; this is format identification, not
authentication against hostile file replacement.

`claim(key, fingerprint, *, now_ms, lease_ms) -> dict` returns exactly one shape:

| Condition | Result |
| --- | --- |
| New key | `{"status": "claimed", "token": "<64 lowercase hex>", "expires_at": now_ms + lease_ms}` |
| Different fingerprint, even after expiry or completion | `{"status": "conflict"}` |
| Same fingerprint, completed | `{"status": "replay", "result": <decoded JSON value>}` |
| Same fingerprint, pending and `now_ms < expires_at` | `{"status": "busy", "expires_at": <existing expiry>}` |
| Same fingerprint, pending and `now_ms >= expires_at` | A new `claimed` result with a fresh token and expiry |

Reclaiming preserves the original fingerprint. Completion never expires, and
every replay returns newly decoded data independent of earlier returned objects.

`complete(key, token, result, *, now_ms) -> bool` stores a result only if the key
is pending, its token matches, and `now_ms < expires_at`. It returns `False` for
missing claims, stale tokens, expired claims, and already completed claims.
It validates the result even when a claim is missing or stale. The return value
reports only whether this ledger accepted the result.

`renew(key, token, *, now_ms, lease_ms) -> bool` explicitly renews a pending
claim only when its token matches and `now_ms < expires_at`. Its new expiry is
`max(previous_expiry, now_ms + lease_ms)`: a shorter or equal request succeeds
without shortening the lease. The token, fingerprint, state, and result stay
intact. It returns `False` for missing, stale, expired, or completed claims;
renewal at the exact expiry cannot revive a claim, including after reclaim.
All key, token, time, lease, and overflow validation runs before any transaction.

Long jobs can call `renew` cooperatively between bounded pieces of work, before
the current lease expires. If it returns `False`, stop starting work under that
claim and discard its result. The [synthetic long-job example](examples/synthetic_long_job.py)
shows these explicit calls with caller-supplied timestamps. Each lease request
is limited to one day, but repeated renewals can extend total ownership beyond
one day. There is no automatic renewal, scheduler, or background clock access.

Invalid inputs and database failures raise `LedgerError` with messages that omit
paths, SQL, and payloads. An invalid input causes no ledger changes.

### Input limits

| Input | Accepted values |
| --- | --- |
| `key` | Exact `str`, ASCII `[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}` |
| `fingerprint`, `token` | Exact `str`, 64 lowercase hexadecimal characters |
| `now_ms` | Exact `int` (not `bool`), `0..2**63-1` |
| `lease_ms` | Exact `int` (not `bool`), `1..86_400_000`; `now_ms + lease_ms <= 2**63-1` |
| `result` | Exact JSON-native types: `None`, `bool`, `int`, finite `float`, `str`, `list`, or `dict` with exact string keys |

Results reject tuples, subclasses of the accepted types, non-finite numbers,
Unicode surrogate code points, and cycles. Shared non-cyclic substructures are
allowed. Up to 32 nested containers are accepted (a root list/dict counts as one),
and the compact encoded UTF-8 JSON must fit in 65,536 bytes, including escaping
and punctuation. Large integers within that limit work without changing Python's
global decimal-digit settings.

## Concurrency and limits

Independent instances, threads, and processes coordinate through SQLite
`BEGIN IMMEDIATE` transactions. Connections close after every operation; there
is no background thread, server, network call, scheduler, automatic execution,
retention policy, or automatic lease renewal. Lock contention waits up to approximately
2 seconds at each SQLite lock acquisition, then raises `LedgerError`; application
retries need their own policy.

Use a dedicated database on a **local trusted filesystem** for modest workloads.
SQLite serializes writers. This library is not a distributed lock service, is
not intended for network filesystems, and has no production fleet integration.
Do not modify its tables or switch its journal mode externally. Completed keys
accumulate permanently; plan disk capacity and lifecycle management yourself.
Treat the database and stored results as sensitive application data.

**There is no exactly-once external-effect guarantee.** Expiring a claim does not
stop the old worker. A worker may perform an external effect and crash before
storing its result; a retry may repeat that effect. Tokens fence writes to this
ledger only. External services or your own atomic business transaction must
provide their own idempotency or fencing. Caller-supplied time is trusted; a
caller supplying a past time can defeat real-world lease expiry.
Renewal does not stop an expired worker or fence downstream effects, and does
not add an exactly-once guarantee.

## Development

```sh
python -B -m unittest discover -s tests -v
```

The public tests cover thread and subprocess claim races, completion races,
expired-owner fencing, permanent replay, fingerprint conflicts, strict JSON and
time boundaries, size/depth limits, bounded contention, private creation mode,
no-overwrite creation races, connection cleanup, injected creation failures,
byte preservation of unrelated databases, and explicit renewal racing reclaim
and completion. Every database fixture is synthetic
and temporary. GitHub Actions runs Python 3.10, 3.12, and 3.14 with read-only
repository permissions and without persisted checkout credentials.

MIT © 2026 Martin Lofrano.
