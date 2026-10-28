"""Cooperative abandonment using synthetic temporary ledgers and real races."""

import concurrent.futures
import hashlib
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from idempotency_ledger import Ledger, LedgerError


FP = hashlib.sha256(b"synthetic cancelled work").hexdigest()
OTHER = hashlib.sha256(b"different work").hexdigest()
MAX_TIME = 2**63 - 1


class AbandonTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="synthetic-abandon-")
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "ledger.sqlite3"
        self.ledger = Ledger.create(self.path)

    def claim(self, key="job", now_ms=100, lease_ms=100):
        return self.ledger.claim(key, FP, now_ms=now_ms, lease_ms=lease_ms)

    def row(self, key="job"):
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

    def test_abandon_expires_only_the_owned_row_and_allows_immediate_reclaim(self):
        first = self.claim()
        self.claim("unrelated")
        before, unrelated = self.row(), self.row("unrelated")
        self.assertIs(self.ledger.abandon("job", first["token"], now_ms=150), True)
        self.assertEqual(self.row(), before[:3] + (0,) + before[4:])
        self.assertEqual(self.row("unrelated"), unrelated)
        self.assertEqual(self.ledger.claim("job", OTHER, now_ms=150, lease_ms=10),
                         {"status": "conflict"})
        second = self.claim(now_ms=150)
        self.assertEqual(second["status"], "claimed")
        self.assertEqual(second["expires_at"], 250)
        self.assertNotEqual(second["token"], first["token"])
        self.assertFalse(self.ledger.complete("job", first["token"], "stale", now_ms=151))
        self.assertTrue(self.ledger.complete("job", second["token"], "accepted", now_ms=151))
        self.assertEqual(self.claim(now_ms=1000), {"status": "replay", "result": "accepted"})

    def test_old_token_is_fenced_even_with_a_past_time(self):
        token = self.claim()["token"]
        self.assertTrue(self.ledger.abandon("job", token, now_ms=150))
        before = self.path.read_bytes()
        for now in (0, 99, 100, 149, 150, 200, MAX_TIME - 1):
            with self.subTest(now=now):
                self.assertFalse(self.ledger.complete("job", token, None, now_ms=now))
                self.assertFalse(self.ledger.renew("job", token, now_ms=now, lease_ms=1))
                self.assertFalse(self.ledger.abandon("job", token, now_ms=now))
                self.assertEqual(self.path.read_bytes(), before)

    def test_missing_stale_and_expired_owners_cannot_abandon(self):
        token = self.claim()["token"]
        stale = "0" * 64 if token != "0" * 64 else "1" * 64
        before = self.path.read_bytes()
        for key, value, now in (("missing", token, 101), ("job", stale, 101),
                                ("job", token, 200), ("job", token, 201)):
            with self.subTest(key=key, now=now):
                self.assertIs(self.ledger.abandon(key, value, now_ms=now), False)
                self.assertEqual(self.path.read_bytes(), before)
        self.claim(now_ms=200)
        replacement = self.row()
        self.assertFalse(self.ledger.abandon("job", token, now_ms=199))
        self.assertEqual(self.row(), replacement)

    def test_completed_result_and_conflict_are_permanent(self):
        token = self.claim()["token"]
        self.assertTrue(self.ledger.complete("job", token, {"done": True}, now_ms=101))
        before = self.path.read_bytes()
        for now in (0, 102, 200, MAX_TIME):
            self.assertIs(self.ledger.abandon("job", token, now_ms=now), False)
            self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.claim(now_ms=1000), {"status": "replay", "result": {"done": True}})
        self.assertEqual(self.ledger.claim("job", OTHER, now_ms=1000, lease_ms=1),
                         {"status": "conflict"})

    def test_invalid_inputs_are_rejected_before_a_transaction(self):
        class Text(str):
            pass

        class Integer(int):
            pass

        token = self.claim()["token"]
        valid = {"key": "job", "token": token, "now_ms": 101}
        invalid = {
            "key": (None, b"job", "", " bad", "á", "a" * 129, Text("job")),
            "token": (None, b"0" * 64, "0" * 63, "0" * 65, "A" * 64, "g" * 64, Text(token)),
            "now_ms": (None, True, 1.0, -1, MAX_TIME + 1, Integer(101)),
        }
        before = self.path.read_bytes()
        with mock.patch.object(self.ledger, "_transaction") as transaction:
            for field, values in invalid.items():
                for value in values:
                    with self.subTest(field=field, value=value):
                        with self.assertRaises(LedgerError):
                            self.ledger.abandon(**dict(valid, **{field: value}))
            transaction.assert_not_called()
        self.assertEqual(self.path.read_bytes(), before)

    def test_zero_and_maximum_time_boundaries(self):
        first = self.claim("zero", now_ms=0, lease_ms=1)
        self.assertTrue(self.ledger.abandon("zero", first["token"], now_ms=0))
        self.assertEqual(self.claim("zero", now_ms=0)["status"], "claimed")
        last = self.claim("last", now_ms=MAX_TIME - 1, lease_ms=1)
        self.assertTrue(self.ledger.abandon("last", last["token"], now_ms=MAX_TIME - 1))
        self.assertEqual(self.claim("last", now_ms=MAX_TIME - 1, lease_ms=1)["status"], "claimed")
        boundary = self.path.read_bytes()
        self.assertFalse(self.ledger.abandon("last", self.row("last")[2], now_ms=MAX_TIME))
        self.assertEqual(self.path.read_bytes(), boundary)

    def test_two_abandoners_have_one_winner(self):
        for index in range(8):
            key = f"abandon:{index}"
            token = self.claim(key)["token"]
            outcomes = self.race(
                lambda ledger: ledger.abandon(key, token, now_ms=150),
                lambda ledger: ledger.abandon(key, token, now_ms=150),
            )
            self.assertEqual(sorted(outcomes), [False, True])
            self.assertEqual(self.row(key)[3:], (0, "pending", None))

    def test_abandon_racing_completion_has_one_consistent_winner(self):
        for index in range(12):
            key = f"complete:{index}"
            token = self.claim(key)["token"]
            abandoned, completed = self.race(
                lambda ledger: ledger.abandon(key, token, now_ms=150),
                lambda ledger: ledger.complete(key, token, {"part": index}, now_ms=150),
            )
            self.assertEqual((abandoned, completed).count(True), 1)
            if completed:
                self.assertEqual(self.claim(key, now_ms=150),
                                 {"status": "replay", "result": {"part": index}})
            else:
                replacement = self.claim(key, now_ms=150)
                self.assertEqual(replacement["status"], "claimed")
                self.assertNotEqual(replacement["token"], token)

    def test_racing_claim_is_busy_or_obtains_released_ownership(self):
        for index in range(12):
            key = f"claim:{index}"
            token = self.claim(key)["token"]
            abandoned, claimed = self.race(
                lambda ledger: ledger.abandon(key, token, now_ms=150),
                lambda ledger: ledger.claim(key, FP, now_ms=150, lease_ms=100),
            )
            self.assertIs(abandoned, True)
            if claimed["status"] == "busy":
                self.assertEqual(claimed, {"status": "busy", "expires_at": 200})
                self.assertEqual(self.row(key)[3], 0)
            else:
                self.assertEqual(claimed["status"], "claimed")
                self.assertNotEqual(claimed["token"], token)
                self.assertEqual(self.row(key)[2:4], (claimed["token"], 250))
            self.assertFalse(self.ledger.complete(key, token, "old", now_ms=0))

    def test_renewal_cannot_revive_an_abandoned_claim(self):
        for index in range(8):
            key = f"renew:{index}"
            token = self.claim(key)["token"]
            abandoned, renewed = self.race(
                lambda ledger: ledger.abandon(key, token, now_ms=150),
                lambda ledger: ledger.renew(key, token, now_ms=150, lease_ms=1000),
            )
            self.assertIs(abandoned, True)
            self.assertIs(type(renewed), bool)
            self.assertEqual(self.row(key)[3:], (0, "pending", None))

    def test_failed_commit_rolls_back_and_closes_connection(self):
        import idempotency_ledger as module

        token = self.claim()["token"]
        real_connect = module._connect
        writes = []

        class FailCommit:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, *args):
                if statement == "COMMIT":
                    raise sqlite3.OperationalError("PRIVATE /path/sql")
                return self.connection.execute(statement, *args)

            def close(self):
                self.connection.close()

        def connect(path, *, readonly=False):
            connection = real_connect(path, readonly=readonly)
            if readonly:
                return connection
            writes.append(connection)
            return FailCommit(connection)

        before = self.row()
        with mock.patch("idempotency_ledger._connect", side_effect=connect):
            with self.assertRaises(LedgerError) as caught:
                self.ledger.abandon("job", token, now_ms=150)
        self.assertNotIn("PRIVATE", str(caught.exception))
        self.assertNotIn("/path", str(caught.exception))
        self.assertEqual(self.row(), before)
        self.assertEqual(len(writes), 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            writes[0].execute("SELECT 1")
        self.assertTrue(self.ledger.complete("job", token, "retained", now_ms=151))


if __name__ == "__main__":
    unittest.main()
