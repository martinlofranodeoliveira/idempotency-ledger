"""Explicit renewal regressions using synthetic, temporary SQLite databases."""

import concurrent.futures
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from idempotency_ledger import Ledger, LedgerError


FP = hashlib.sha256(b"synthetic long job").hexdigest()
MAX_TIME = 2**63 - 1
DAY = 86_400_000


class RenewalTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="synthetic-renewal-")
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "ledger.sqlite3"
        self.ledger = Ledger.create(self.path)

    def claim(self, key="job:1", now_ms=100, lease_ms=100):
        return self.ledger.claim(key, FP, now_ms=now_ms, lease_ms=lease_ms)

    def row(self, key="job:1"):
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                "SELECT key, fingerprint, token, expires_at, state, result "
                "FROM _ledger_entries WHERE key = ?", (key,)
            ).fetchone()

    def race(self, first, second):
        barrier = threading.Barrier(2)

        def run(operation):
            ledger = Ledger(self.path)
            barrier.wait(timeout=5)
            return operation(ledger)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(run, operation) for operation in (first, second)]
            return tuple(future.result(timeout=10) for future in futures)

    def test_extension_preserves_identity_and_all_other_entry_fields(self):
        claim = self.claim()
        before = self.row()
        with sqlite3.connect(self.path) as connection:
            identity = connection.execute("SELECT * FROM _ledger_meta").fetchall()
            schema = connection.execute("SELECT * FROM sqlite_master").fetchall()
        self.assertTrue(self.ledger.renew("job:1", claim["token"], now_ms=199, lease_ms=101))
        self.assertEqual(self.row(), before[:3] + (300,) + before[4:])
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT * FROM _ledger_meta").fetchall(), identity)
            self.assertEqual(connection.execute("SELECT * FROM sqlite_master").fetchall(), schema)
        self.assertEqual(self.claim(now_ms=250), {"status": "busy", "expires_at": 300})
        self.assertTrue(self.ledger.complete("job:1", claim["token"], {"parts": 3}, now_ms=299))
        self.assertEqual(self.claim(now_ms=MAX_TIME - 100),
                         {"status": "replay", "result": {"parts": 3}})

    def test_shorter_and_equal_requests_succeed_without_shortening(self):
        token = self.claim()["token"]
        before = self.row()
        for now_ms, lease_ms in ((101, 1), (101, 99), (0, 1)):
            with self.subTest(now_ms=now_ms, lease_ms=lease_ms):
                self.assertTrue(self.ledger.renew("job:1", token, now_ms=now_ms, lease_ms=lease_ms))
                self.assertEqual(self.row(), before)

    def test_missing_stale_and_expired_claims_cannot_renew(self):
        token = self.claim()["token"]
        stale = "0" * 64 if token != "0" * 64 else "1" * 64
        before = self.path.read_bytes()
        for key, candidate, now_ms in (("missing", token, 101), ("job:1", stale, 101),
                                       ("job:1", token, 200), ("job:1", token, 201)):
            with self.subTest(key=key, now_ms=now_ms):
                self.assertFalse(self.ledger.renew(key, candidate, now_ms=now_ms, lease_ms=100))
                self.assertEqual(self.path.read_bytes(), before)
        replacement = self.claim(now_ms=200)
        self.assertNotEqual(replacement["token"], token)
        replaced = self.row()
        self.assertFalse(self.ledger.renew("job:1", token, now_ms=199, lease_ms=500))
        self.assertEqual(self.row(), replaced)
        self.assertFalse(self.ledger.complete("job:1", token, "stale", now_ms=201))
        self.assertTrue(self.ledger.renew("job:1", replacement["token"], now_ms=201, lease_ms=100))

    def test_completed_claim_cannot_renew_or_change_permanent_replay(self):
        token = self.claim()["token"]
        result = {"completed": [True, None, "synthetic"]}
        self.assertTrue(self.ledger.complete("job:1", token, result, now_ms=101))
        before = self.path.read_bytes()
        for now_ms in (0, 102, 200, MAX_TIME - DAY):
            self.assertFalse(self.ledger.renew("job:1", token, now_ms=now_ms, lease_ms=DAY))
            self.assertEqual(self.path.read_bytes(), before)
            self.assertEqual(self.claim(now_ms=now_ms), {"status": "replay", "result": result})

    def test_repeated_renewals_can_exceed_a_day_and_reach_maximum_time(self):
        token = self.claim(now_ms=0, lease_ms=DAY)["token"]
        for now_ms in (DAY - 1, 2 * DAY - 2):
            self.assertTrue(self.ledger.renew("job:1", token, now_ms=now_ms, lease_ms=DAY))
            self.assertEqual(self.row()[3], now_ms + DAY)
        latest = self.claim("job:max", now_ms=MAX_TIME - 100, lease_ms=50)
        self.assertTrue(self.ledger.renew("job:max", latest["token"],
                                          now_ms=MAX_TIME - 60, lease_ms=60))
        self.assertEqual(self.row("job:max")[3], MAX_TIME)
        self.assertTrue(self.ledger.complete("job:max", latest["token"], "ok", now_ms=MAX_TIME - 1))

    def test_invalid_inputs_raise_before_any_transaction_without_mutation(self):
        class Text(str):
            pass

        class Integer(int):
            pass

        token = self.claim()["token"]
        valid = {"key": "job:1", "token": token, "now_ms": 101, "lease_ms": 100}
        invalid = {
            "key": (None, b"job:1", "", " bad", "á", "a" * 129, Text("job:1")),
            "token": (None, b"0" * 64, "0" * 63, "0" * 65, "A" * 64, "g" * 64, Text(token)),
            "now_ms": (None, True, 1.0, -1, MAX_TIME + 1, Integer(101)),
            "lease_ms": (None, True, 1.0, 0, -1, DAY + 1, Integer(100)),
        }
        before = self.path.read_bytes()
        with mock.patch.object(self.ledger, "_transaction") as transaction:
            for field, values in invalid.items():
                for value in values:
                    with self.subTest(field=field, value=value):
                        arguments = dict(valid, **{field: value})
                        with self.assertRaises(LedgerError):
                            self.ledger.renew(**arguments)
            with self.assertRaises(LedgerError):
                self.ledger.renew("job:1", token, now_ms=MAX_TIME, lease_ms=1)
            transaction.assert_not_called()
        self.assertEqual(self.path.read_bytes(), before)

    def test_concurrent_renewals_keep_the_longest_expiry(self):
        token = self.claim()["token"]
        first, second = self.race(
            lambda ledger: ledger.renew("job:1", token, now_ms=199, lease_ms=501),
            lambda ledger: ledger.renew("job:1", token, now_ms=198, lease_ms=102),
        )
        self.assertIs(first, True)
        self.assertIs(second, True)
        self.assertEqual(self.row()[3], 700)

    def test_renewal_racing_reclaim_has_one_consistent_owner(self):
        for index in range(12):
            key = f"race:{index}"
            token = self.claim(key)["token"]
            renewed, reclaimed = self.race(
                lambda ledger: ledger.renew(key, token, now_ms=199, lease_ms=101),
                lambda ledger: ledger.claim(key, FP, now_ms=200, lease_ms=100),
            )
            with self.subTest(index=index):
                row = self.row(key)
                self.assertEqual(row[3:], (300, "pending", None))
                if renewed:
                    self.assertEqual(reclaimed, {"status": "busy", "expires_at": 300})
                    self.assertEqual(row[2], token)
                else:
                    self.assertEqual(reclaimed["status"], "claimed")
                    self.assertNotEqual(reclaimed["token"], token)
                    self.assertEqual(row[2], reclaimed["token"])
                    self.assertFalse(self.ledger.renew(key, token, now_ms=199, lease_ms=500))

    def test_renewal_racing_completion_preserves_the_completed_result(self):
        for index in range(12):
            key = f"completion:{index}"
            token = self.claim(key)["token"]
            result = {"synthetic_part": index}
            renewed, completed = self.race(
                lambda ledger: ledger.renew(key, token, now_ms=199, lease_ms=101),
                lambda ledger: ledger.complete(key, token, result, now_ms=199),
            )
            with self.subTest(index=index):
                self.assertIs(completed, True)
                self.assertEqual(self.row(key)[3], 300 if renewed else 200)
                self.assertFalse(self.ledger.renew(key, token, now_ms=199, lease_ms=500))
                self.assertEqual(self.claim(key, now_ms=1000), {"status": "replay", "result": result})


if __name__ == "__main__":
    unittest.main()
