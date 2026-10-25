"""Public contract tests. All databases are synthetic temporary fixtures."""

import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from idempotency_ledger import Ledger, LedgerError


FP = hashlib.sha256(b"synthetic request").hexdigest()
OTHER_FP = hashlib.sha256(b"different synthetic request").hexdigest()
TOKEN = "0" * 64
MAX_TIME = 2**63 - 1


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-ledger-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.path = self.directory / "ledger.sqlite3"
        self.ledger = Ledger.create(self.path)

    def claim(self, **kwargs):
        return self.ledger.claim("request:1", FP, now_ms=kwargs.get("now_ms", 100),
                                 lease_ms=kwargs.get("lease_ms", 50))

    def test_claim_busy_expiry_reclaim_and_fencing(self):
        first = self.claim()
        self.assertEqual(set(first), {"status", "token", "expires_at"})
        self.assertEqual(first["status"], "claimed")
        self.assertRegex(first["token"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["expires_at"], 150)
        self.assertEqual(self.claim(now_ms=149), {"status": "busy", "expires_at": 150})
        second = self.claim(now_ms=150)
        self.assertNotEqual(first["token"], second["token"])
        self.assertFalse(self.ledger.complete("request:1", first["token"], "stale", now_ms=151))
        self.assertTrue(self.ledger.complete("request:1", second["token"], "winner", now_ms=151))
        self.assertEqual(self.claim(now_ms=MAX_TIME - 50), {"status": "replay", "result": "winner"})

    def test_completion_boundary_and_missing_claim(self):
        first = self.claim()
        self.assertFalse(self.ledger.complete("request:1", first["token"], None, now_ms=150))
        self.assertFalse(self.ledger.complete("request:1", first["token"], None, now_ms=151))
        self.assertFalse(self.ledger.complete("missing", TOKEN, None, now_ms=100))
        self.assertFalse(self.ledger.complete("request:1", TOKEN, None, now_ms=101))
        self.assertTrue(self.ledger.complete("request:1", first["token"], None, now_ms=149))
        self.assertFalse(self.ledger.complete("request:1", first["token"], "replacement", now_ms=149))
        self.assertEqual(self.claim(), {"status": "replay", "result": None})

    def test_fingerprint_conflict_survives_expiry_reclaim_and_completion(self):
        first = self.claim()
        for now in (101, 150, 200):
            self.assertEqual(self.ledger.claim("request:1", OTHER_FP, now_ms=now, lease_ms=5),
                             {"status": "conflict"})
        second = self.claim(now_ms=150)
        self.assertFalse(self.ledger.complete("request:1", first["token"], None, now_ms=151))
        self.assertTrue(self.ledger.complete("request:1", second["token"], {"ok": True}, now_ms=151))
        self.assertEqual(self.ledger.claim("request:1", OTHER_FP, now_ms=1000, lease_ms=5),
                         {"status": "conflict"})

    def test_results_are_detached_on_write_and_every_replay(self):
        token = self.claim()["token"]
        original = {"items": [None, True, False, 42, -42, 1.25, "Olá 🌎"], "nested": {"x": []}}
        expected = json.loads(json.dumps(original))
        self.assertTrue(self.ledger.complete("request:1", token, original, now_ms=101))
        original["items"].append("mutated")
        first = self.claim()["result"]
        self.assertEqual(first, expected)
        first["nested"]["x"].append(1)
        self.assertEqual(self.claim()["result"], expected)
        self.assertEqual(Ledger(self.path).claim("request:1", FP, now_ms=999, lease_ms=1)["result"], expected)

    def test_exact_types_and_json_edge_values(self):
        class IntSubclass(int):
            pass

        class StringSubclass(str):
            pass

        invalid = [(1, 2), {1: "key"}, float("nan"), float("inf"), -float("inf"),
                   b"bytes", {"x": object()}, {"x": (1,)}, "\ud800", {"\udfff": 1},
                   IntSubclass(1), StringSubclass("text"), {StringSubclass("key"): 1},
                   {"x": {1, 2}}]
        token = self.claim()["token"]
        before = self.path.read_bytes()
        for result in invalid:
            with self.subTest(result_type=type(result).__name__):
                with self.assertRaises(LedgerError):
                    self.ledger.complete("request:1", token, result, now_ms=101)
                self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(self.ledger.complete("request:1", token, {"still": "pending"}, now_ms=101))

    def test_cycle_and_shared_substructure(self):
        token = self.claim()["token"]
        cyclic_list = []
        cyclic_list.append(cyclic_list)
        cyclic_dict = {}
        cyclic_dict["self"] = cyclic_dict
        for result in (cyclic_list, cyclic_dict):
            with self.assertRaises(LedgerError):
                self.ledger.complete("request:1", token, result, now_ms=101)
        shared = [1]
        self.assertTrue(self.ledger.complete("request:1", token, [shared, shared], now_ms=101))
        self.assertEqual(self.claim()["result"], [[1], [1]])

    def test_nesting_limit(self):
        token = self.claim()["token"]
        value = 0
        for _ in range(32):
            value = [value]
        with self.assertRaises(LedgerError):
            self.ledger.complete("request:1", token, [value], now_ms=101)
        self.assertTrue(self.ledger.complete("request:1", token, value, now_ms=101))
        self.assertEqual(self.claim()["result"], value)

    def test_encoded_size_includes_utf8_quotes_and_escaping(self):
        for result in ("a" * 65535, "é" * 32768, "\n" * 32768):
            with self.subTest(length=len(result)):
                with self.assertRaises(LedgerError):
                    self.ledger.complete("missing", TOKEN, result, now_ms=0)
        token = self.claim()["token"]
        exact = "é" * 32767  # 65534 UTF-8 bytes plus JSON quotes.
        self.assertTrue(self.ledger.complete("request:1", token, exact, now_ms=101))
        self.assertEqual(self.claim()["result"], exact)

    def test_large_native_integer_without_global_digit_limit_changes(self):
        token = self.claim()["token"]
        before = sys.get_int_max_str_digits() if hasattr(sys, "get_int_max_str_digits") else None
        value = 10**5000 + 17
        self.assertTrue(self.ledger.complete("request:1", token, value, now_ms=101))
        self.assertEqual(self.claim()["result"], value)
        after = sys.get_int_max_str_digits() if hasattr(sys, "get_int_max_str_digits") else None
        self.assertEqual(before, after)
        with self.assertRaises(LedgerError):
            self.ledger.complete("missing", TOKEN, 10**65536, now_ms=0)

    def test_invalid_results_rejected_even_for_missing_or_stale_claims(self):
        for key in ("missing", "request:1"):
            with self.assertRaises(LedgerError):
                self.ledger.complete(key, TOKEN, (), now_ms=0)

    def test_key_rules_and_boundaries(self):
        invalid = ["", " a", "a b", "é", "_start", "/start", "a\n", "a\x00", "a" * 129, 12, True]
        before = self.path.read_bytes()
        for key in invalid:
            with self.subTest(key=repr(key)):
                with self.assertRaises(LedgerError):
                    self.ledger.claim(key, FP, now_ms=0, lease_ms=1)
                with self.assertRaises(LedgerError):
                    self.ledger.complete(key, TOKEN, None, now_ms=0)
                self.assertEqual(self.path.read_bytes(), before)
        for key in ("0", "A:z._/-", "a" * 128):
            self.assertEqual(self.ledger.claim(key, FP, now_ms=0, lease_ms=1)["status"], "claimed")

    def test_fingerprint_and_token_rules(self):
        before = self.path.read_bytes()
        for value in ("", "f" * 63, "f" * 65, "F" * 64, "g" * 64, "a" * 63 + "\n", 1, True):
            with self.subTest(value=repr(value)):
                with self.assertRaises(LedgerError):
                    self.ledger.claim("key", value, now_ms=0, lease_ms=1)
                with self.assertRaises(LedgerError):
                    self.ledger.complete("key", value, None, now_ms=0)
                self.assertEqual(self.path.read_bytes(), before)

    def test_time_and_lease_rules(self):
        before = self.path.read_bytes()
        for value in (-1, MAX_TIME + 1, True, 1.0, "1", None):
            with self.subTest(time=value):
                with self.assertRaises(LedgerError):
                    self.ledger.claim("key", FP, now_ms=value, lease_ms=1)
                with self.assertRaises(LedgerError):
                    self.ledger.complete("key", TOKEN, None, now_ms=value)
        for value in (0, -1, 86_400_001, True, 1.0, "1", None):
            with self.subTest(lease=value):
                with self.assertRaises(LedgerError):
                    self.ledger.claim("key", FP, now_ms=0, lease_ms=value)
        with self.assertRaises(LedgerError):
            self.ledger.claim("key", FP, now_ms=MAX_TIME, lease_ms=1)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.ledger.claim("max", FP, now_ms=MAX_TIME - 1, lease_ms=1)["expires_at"], MAX_TIME)
        self.assertEqual(self.ledger.claim("lease", FP, now_ms=0, lease_ms=86_400_000)["expires_at"], 86_400_000)

    def test_threads_have_exactly_one_claim_winner(self):
        workers = 16
        barrier = threading.Barrier(workers)

        def compete(index):
            instance = self.ledger if index % 2 else Ledger(self.path)
            barrier.wait(timeout=10)
            return instance.claim("thread-race", FP, now_ms=100, lease_ms=1000)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            outcomes = list(pool.map(compete, range(workers)))
        self.assertEqual(sum(x["status"] == "claimed" for x in outcomes), 1)
        self.assertEqual(sum(x == {"status": "busy", "expires_at": 1100} for x in outcomes), workers - 1)

    def test_independent_processes_have_exactly_one_claim_winner(self):
        program = """
import json, sys
from idempotency_ledger import Ledger
ledger = Ledger(sys.argv[1])
print('ready', flush=True)
sys.stdin.readline()
print(json.dumps(ledger.claim('process-race', sys.argv[2], now_ms=100, lease_ms=1000)), flush=True)
"""
        children = []
        try:
            for _ in range(8):
                children.append(subprocess.Popen(
                    [sys.executable, "-B", "-c", program, str(self.path), FP],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True,
                ))
            for child in children:
                self.assertEqual(child.stdout.readline().strip(), "ready")
            for child in children:
                child.stdin.write("go\n")
                child.stdin.flush()
            outcomes = []
            for child in children:
                output, error = child.communicate(timeout=15)
                self.assertEqual(child.returncode, 0, error)
                outcomes.append(json.loads(output))
            self.assertEqual(sum(x["status"] == "claimed" for x in outcomes), 1)
            self.assertEqual(sum(x == {"status": "busy", "expires_at": 1100} for x in outcomes), 7)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate()

    def test_threads_have_one_completion_winner(self):
        token = self.claim()["token"]
        barrier = threading.Barrier(8)

        def complete(index):
            barrier.wait(timeout=10)
            return index, Ledger(self.path).complete("request:1", token, {"worker": index}, now_ms=101)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(complete, range(8)))
        winners = [index for index, won in outcomes if won]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.claim(), {"status": "replay", "result": {"worker": winners[0]}})

    def test_expired_reclaim_race_has_one_new_owner(self):
        old = self.claim()["token"]
        barrier = threading.Barrier(8)

        def reclaim(_):
            barrier.wait(timeout=10)
            return Ledger(self.path).claim("request:1", FP, now_ms=150, lease_ms=50)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(reclaim, range(8)))
        winners = [result for result in outcomes if result["status"] == "claimed"]
        self.assertEqual(len(winners), 1)
        self.assertNotEqual(winners[0]["token"], old)
        self.assertFalse(self.ledger.complete("request:1", old, "stale", now_ms=151))
        self.assertTrue(self.ledger.complete("request:1", winners[0]["token"], "fresh", now_ms=151))

    def test_busy_timeout_is_bounded_and_does_not_mutate(self):
        connection = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(connection.close)
        connection.execute("BEGIN IMMEDIATE")
        before = self.path.read_bytes()
        started = time.monotonic()
        with self.assertRaises(LedgerError) as caught:
            self.claim()
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 1)
        self.assertLess(elapsed, 6)
        self.assertNotIn(str(self.path), str(caught.exception))
        self.assertEqual(before, self.path.read_bytes())
        connection.rollback()
        self.assertEqual(self.claim()["status"], "claimed")

    def test_existing_file_directory_and_broken_link_are_never_overwritten(self):
        targets = [self.path, self.directory / "plain", self.directory / "folder", self.directory / "broken"]
        targets[1].write_bytes(b"synthetic foreign data")
        targets[2].mkdir()
        targets[3].symlink_to(self.directory / "does-not-exist")
        before = {p.name: (p.lstat().st_mode, p.read_bytes() if p.is_file() else None) for p in targets}
        names = set(self.directory.iterdir())
        for target in targets:
            with self.subTest(target=target.name):
                with self.assertRaises(LedgerError):
                    Ledger.create(target)
        self.assertEqual(set(self.directory.iterdir()), names)
        for target in targets:
            self.assertEqual(target.lstat().st_mode, before[target.name][0])
            if target.is_file():
                self.assertEqual(target.read_bytes(), before[target.name][1])
        self.assertTrue(targets[3].is_symlink())

    def test_simultaneous_create_has_one_winner_and_no_staging_left(self):
        path = self.directory / "create-race.db"
        barrier = threading.Barrier(8)

        def create(_):
            barrier.wait(timeout=10)
            try:
                Ledger.create(path)
                return True
            except LedgerError:
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(create, range(8))), 1)
        Ledger(path)
        self.assertEqual({p.name for p in self.directory.iterdir()}, {self.path.name, path.name})

    def test_new_file_is_private_even_with_permissive_umask(self):
        old_umask = os.umask(0)
        try:
            path = self.directory / "mode.db"
            Ledger.create(path)
        finally:
            os.umask(old_umask)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_constructor_never_initializes_missing_or_foreign_database(self):
        ordinary = self.directory / "ordinary.db"
        connection = sqlite3.connect(ordinary)
        connection.execute("CREATE TABLE invoices (id INTEGER, note TEXT)")
        connection.execute("INSERT INTO invoices VALUES (1, 'synthetic')")
        connection.commit()
        connection.close()
        text = self.directory / "text"
        text.write_bytes(b"not sqlite: synthetic fixture")
        for path in (ordinary, text, self.directory / "missing"):
            before = path.read_bytes() if path.exists() else None
            names = set(self.directory.iterdir())
            with self.assertRaises(LedgerError) as caught:
                Ledger(path)
            self.assertNotIn(str(path), str(caught.exception))
            self.assertEqual(set(self.directory.iterdir()), names)
            self.assertEqual(path.read_bytes() if path.exists() else None, before)

    def test_replaced_database_is_preserved_before_operation(self):
        self.path.unlink()
        connection = sqlite3.connect(self.path)
        connection.execute("CREATE TABLE synthetic (value TEXT)")
        connection.execute("INSERT INTO synthetic VALUES ('preserve')")
        connection.commit()
        connection.close()
        before = self.path.read_bytes()
        names = set(self.directory.iterdir())
        with self.assertRaises(LedgerError):
            self.claim()
        with self.assertRaises(LedgerError):
            self.ledger.complete("request:1", TOKEN, None, now_ms=0)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(set(self.directory.iterdir()), names)

    def test_identification_rejects_tampered_schema_and_metadata(self):
        for sql in ("PRAGMA application_id = 0", "PRAGMA user_version = 2",
                    "UPDATE _ledger_meta SET identity = 'foreign'",
                    "CREATE TABLE unrelated (value TEXT)"):
            with self.subTest(sql=sql):
                path = self.directory / (str(len(list(self.directory.iterdir()))) + ".db")
                Ledger.create(path)
                with sqlite3.connect(path) as connection:
                    connection.execute(sql)
                before = path.read_bytes()
                with self.assertRaises(LedgerError):
                    Ledger(path)
                self.assertEqual(path.read_bytes(), before)

    def test_creation_failure_cleans_staging_and_sanitizes_error(self):
        path = self.directory / "failure.db"
        before = set(self.directory.iterdir())
        with mock.patch("idempotency_ledger.os.link", side_effect=OSError("SECRET SQL /private/path")):
            with self.assertRaises(LedgerError) as caught:
                Ledger.create(path)
        self.assertNotIn("SECRET", str(caught.exception))
        self.assertNotIn("/private", str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertEqual(set(self.directory.iterdir()), before)

    def test_schema_failure_cleans_staging_journal(self):
        before = set(self.directory.iterdir())
        with mock.patch("idempotency_ledger._SCHEMA", {"invalid": "INVALID SQL"}):
            with self.assertRaises(LedgerError):
                Ledger.create(self.directory / "failure.db")
        self.assertEqual(set(self.directory.iterdir()), before)

    def test_invalid_paths_raise_sanitized_errors(self):
        for path in (None, 1, b"bytes", "", "secret\x00path", self.directory / "missing" / "file"):
            with self.subTest(path_type=type(path).__name__):
                with self.assertRaises(LedgerError):
                    Ledger(path)
                with self.assertRaises(LedgerError):
                    Ledger.create(path)

    def test_connections_close_after_success_and_failure(self):
        real_connect = sqlite3.connect
        connections = []

        def track(*args, **kwargs):
            connection = real_connect(*args, **kwargs)
            connections.append(connection)
            return connection

        with mock.patch("idempotency_ledger.sqlite3.connect", side_effect=track):
            Ledger(self.path)
            token = self.claim()["token"]
            self.ledger.complete("request:1", token, {"ok": True}, now_ms=101)
            self.claim()
            with self.assertRaises(LedgerError):
                Ledger(self.directory / "missing")
            with mock.patch("idempotency_ledger._validate", side_effect=LedgerError("invalid")):
                with self.assertRaises(LedgerError):
                    self.claim()
        self.assertGreaterEqual(len(connections), 7)
        for connection in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")

    def test_failed_commit_rolls_back_result_and_closes_connection(self):
        import idempotency_ledger as module

        token = self.claim()["token"]
        real_connect = module._connect
        writes = []

        class FailCommit:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, *args):
                if statement == "COMMIT":
                    raise sqlite3.OperationalError("SECRET /private/path")
                return self.connection.execute(statement, *args)

            def close(self):
                self.connection.close()

        def connect(path, *, readonly=False):
            connection = real_connect(path, readonly=readonly)
            if readonly:
                return connection
            writes.append(connection)
            return FailCommit(connection)

        with mock.patch("idempotency_ledger._connect", side_effect=connect):
            with self.assertRaises(LedgerError) as caught:
                self.ledger.complete("request:1", token, "not committed", now_ms=101)
        self.assertNotIn("SECRET", str(caught.exception))
        for connection in writes:
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
        self.assertEqual(self.claim(), {"status": "busy", "expires_at": 150})
        self.assertTrue(self.ledger.complete("request:1", token, "committed", now_ms=101))
        self.assertEqual(self.claim(), {"status": "replay", "result": "committed"})

    def test_paths_with_uri_characters_work(self):
        path = self.directory / "synthetic ?#% name.db"
        ledger = Ledger.create(path)
        result = ledger.claim("key", FP, now_ms=0, lease_ms=1)
        self.assertTrue(ledger.complete("key", result["token"], "ok", now_ms=0))
        self.assertEqual(Ledger(path).claim("key", FP, now_ms=2, lease_ms=1),
                         {"status": "replay", "result": "ok"})


if __name__ == "__main__":
    unittest.main()
