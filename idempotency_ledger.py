"""SQLite claims with lease fencing and permanent JSON result replay.

The caller supplies time and runs its own work. This module never executes work,
stops an expired worker, or guarantees exactly-once external side effects.
"""

from __future__ import annotations

import json
import math
import os
import re
import secrets
import sqlite3
import tempfile
from contextlib import contextmanager
from typing import Iterator
from urllib.parse import quote

__all__ = ["Ledger", "LedgerError"]

_APPLICATION_ID = 0x49444C47
_SCHEMA_VERSION = 1
_IDENTITY = "idempotency-ledger/v1"
_BUSY_TIMEOUT_MS = 2000
_MAX_TIME = 2**63 - 1
_MAX_LEASE = 86_400_000
_MAX_RESULT_BYTES = 65_536
_MAX_DEPTH = 32
_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/-]{0,127}\Z", re.ASCII)
_HEX = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_SCHEMA = {
    "_ledger_meta": """CREATE TABLE _ledger_meta (
        identity TEXT PRIMARY KEY NOT NULL,
        schema_version INTEGER NOT NULL
    )""",
    "_ledger_entries": """CREATE TABLE _ledger_entries (
        key TEXT PRIMARY KEY NOT NULL,
        fingerprint TEXT NOT NULL,
        token TEXT NOT NULL,
        expires_at INTEGER NOT NULL CHECK(expires_at >= 0),
        state TEXT NOT NULL CHECK(state IN ('pending', 'completed')),
        result TEXT,
        CHECK((state = 'pending' AND result IS NULL)
           OR (state = 'completed' AND result IS NOT NULL))
    )""",
}


class LedgerError(Exception):
    """An invalid input, unrecognized database, or failed ledger operation.

    Messages deliberately omit paths, SQL, keys, fingerprints, and results.
    """


def _path(value: object) -> str:
    try:
        raw = os.fspath(value)
        if type(raw) is not str or not raw or "\x00" in raw:
            raise ValueError
        return os.path.abspath(raw)
    except (TypeError, ValueError, OSError):
        raise LedgerError("Invalid ledger path.") from None


def _text(value: object, pattern: re.Pattern[str], label: str) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise LedgerError(f"Invalid {label}.")
    return value


def _integer(value: object, minimum: int, maximum: int, label: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise LedgerError(f"Invalid {label}.")
    return value


def _int_text(value: int) -> str:
    # Avoid the interpreter's configurable decimal-digit limit without changing
    # process-global settings. Every individual conversion is at most 9 digits.
    if value.bit_length() > _MAX_RESULT_BYTES * 4:
        raise LedgerError("Result exceeds the encoded size limit.")
    negative = value < 0
    value = abs(value)
    parts: list[int] = []
    while value >= 1_000_000_000:
        value, remainder = divmod(value, 1_000_000_000)
        parts.append(remainder)
    return ("-" if negative else "") + str(value) + "".join(
        f"{part:09d}" for part in reversed(parts)
    )


def _parse_int(value: str) -> int:
    negative = value.startswith("-")
    digits = value[1:] if negative else value
    number = 0
    for start in range(0, len(digits), 9):
        chunk = digits[start : start + 9]
        number = number * 10 ** len(chunk) + int(chunk)
    return -number if negative else number


def _encode_result(value: object) -> str:
    pieces: list[str] = []
    active: set[int] = set()
    length = 0

    def append(piece: str) -> None:
        nonlocal length
        try:
            length += len(piece.encode("utf-8"))
        except UnicodeError:
            raise LedgerError("Result contains an invalid string.") from None
        if length > _MAX_RESULT_BYTES:
            raise LedgerError("Result exceeds the encoded size limit.")
        pieces.append(piece)

    def string(item: str) -> None:
        if len(item) > _MAX_RESULT_BYTES:
            raise LedgerError("Result exceeds the encoded size limit.")
        append(json.dumps(item, ensure_ascii=False))

    def visit(item: object, depth: int) -> None:
        kind = type(item)
        if item is None:
            append("null")
        elif kind is bool:
            append("true" if item else "false")
        elif kind is int:
            append(_int_text(item))
        elif kind is float:
            if not math.isfinite(item):
                raise LedgerError("Result contains a non-finite number.")
            append(json.dumps(item, allow_nan=False))
        elif kind is str:
            string(item)
        elif kind is list or kind is dict:
            if depth >= _MAX_DEPTH:
                raise LedgerError("Result exceeds the nesting limit.")
            if id(item) in active:
                raise LedgerError("Result contains a cycle.")
            active.add(id(item))
            try:
                append("[" if kind is list else "{")
                for index, element in enumerate(item):
                    if index:
                        append(",")
                    if kind is dict:
                        if type(element) is not str:
                            raise LedgerError("Result object keys must be strings.")
                        string(element)
                        append(":")
                        visit(item[element], depth + 1)
                    else:
                        visit(element, depth + 1)
                append("]" if kind is list else "}")
            finally:
                active.remove(id(item))
        else:
            raise LedgerError("Result must contain only JSON-native values.")

    try:
        visit(value, 0)
    except (ValueError, TypeError, OverflowError, RecursionError, RuntimeError):
        raise LedgerError("Invalid result.") from None
    return "".join(pieces)


def _connect(path: str, *, readonly: bool = False) -> sqlite3.Connection:
    uri = "file:" + quote(path, safe="/")
    uri += "?mode=ro&immutable=1" if readonly else "?mode=rw"
    connection = sqlite3.connect(
        uri, uri=True, isolation_level=None, timeout=_BUSY_TIMEOUT_MS / 1000
    )
    try:
        connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        return connection
    except BaseException:
        connection.close()
        raise


def _validate(connection: sqlite3.Connection) -> None:
    if (
        connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
        or connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION
    ):
        raise LedgerError("Not a recognized ledger database.")
    schema = connection.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    expected = sorted(
        ("table", name, " ".join(statement.split()))
        for name, statement in _SCHEMA.items()
    )
    actual = [(kind, name, " ".join(sql.split()) if sql else "") for kind, name, sql in schema]
    if actual != expected or connection.execute(
        "SELECT identity, schema_version FROM _ledger_meta"
    ).fetchall() != [(_IDENTITY, _SCHEMA_VERSION)]:
        raise LedgerError("Not a recognized ledger database.")


class Ledger:
    """A ledger at an existing path; connections are scoped to each operation.

    Use ``Ledger.create(path)`` once to create a new database without replacing
    any existing directory entry. The filesystem must be local and trusted.
    """

    def __init__(self, path: str | os.PathLike[str]):
        self._path = _path(path)
        self._check_existing()

    def _check_existing(self) -> None:
        connection = None
        try:
            connection = _connect(self._path, readonly=True)
            _validate(connection)
        except (sqlite3.Error, OSError, ValueError, OverflowError):
            raise LedgerError("Cannot open a recognized ledger database.") from None
        finally:
            if connection is not None:
                connection.close()

    @classmethod
    def create(cls, path: str | os.PathLike[str]) -> Ledger:
        """Atomically publish a mode-0600 database without overwriting anything.

        A same-parent staging file is removed on success or failure. Publishing
        uses ``os.link``, so filesystems without hard-link support are rejected.
        """
        target = _path(path)
        staging: str | None = None
        connection = None
        try:
            descriptor, staging = tempfile.mkstemp(
                prefix=".idempotency-ledger-", dir=os.path.dirname(target)
            )
            os.close(descriptor)
            os.chmod(staging, 0o600)
            connection = _connect(staging)
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
            connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            for statement in _SCHEMA.values():
                connection.execute(statement)
            connection.execute(
                "INSERT INTO _ledger_meta VALUES (?, ?)", (_IDENTITY, _SCHEMA_VERSION)
            )
            connection.execute("COMMIT")
            connection.close()
            connection = None
            os.link(staging, target)
        except (sqlite3.Error, OSError, ValueError, OverflowError):
            raise LedgerError("Cannot create ledger database.") from None
        finally:
            if connection is not None:
                connection.close()
            if staging is not None:
                for suffix in ("", "-journal", "-wal", "-shm"):
                    try:
                        os.unlink(staging + suffix)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        raise LedgerError("Cannot clean up ledger staging file.") from None
        return cls(target)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = None
        try:
            # Immutable read-only inspection cannot initialize or recover an
            # unrelated SQLite file. Repeat identity checks inside the write
            # transaction before making changes.
            self._check_existing()
            connection = _connect(self._path)
            connection.execute("BEGIN IMMEDIATE")
            _validate(connection)
            yield connection
            connection.execute("COMMIT")
        except (sqlite3.Error, OSError, ValueError, OverflowError):
            raise LedgerError("Ledger operation failed.") from None
        finally:
            if connection is not None:
                # Closing an uncommitted transaction rolls it back.
                connection.close()

    def claim(
        self, key: str, fingerprint: str, *, now_ms: int, lease_ms: int
    ) -> dict:
        """Claim, report busy/conflict, or replay a permanently completed result."""
        key = _text(key, _KEY, "key")
        fingerprint = _text(fingerprint, _HEX, "fingerprint")
        now_ms = _integer(now_ms, 0, _MAX_TIME, "time")
        lease_ms = _integer(lease_ms, 1, _MAX_LEASE, "lease")
        if now_ms + lease_ms > _MAX_TIME:
            raise LedgerError("Lease expiry exceeds the time limit.")
        expires_at = now_ms + lease_ms
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT fingerprint, token, expires_at, state, result "
                "FROM _ledger_entries WHERE key = ?", (key,)
            ).fetchone()
            if row is not None:
                previous_fingerprint, previous_token, previous_expiry, state, result = row
                if previous_fingerprint != fingerprint:
                    return {"status": "conflict"}
                if state == "completed":
                    try:
                        decoded = json.loads(result, parse_int=_parse_int)
                        _encode_result(decoded)
                    except (ValueError, TypeError, RecursionError):
                        raise LedgerError("Stored result is invalid.") from None
                    return {"status": "replay", "result": decoded}
                if now_ms < previous_expiry:
                    return {"status": "busy", "expires_at": previous_expiry}
            token = secrets.token_hex(32)
            while row is not None and token == previous_token:
                token = secrets.token_hex(32)
            if row is None:
                connection.execute(
                    "INSERT INTO _ledger_entries VALUES (?, ?, ?, ?, 'pending', NULL)",
                    (key, fingerprint, token, expires_at),
                )
            else:
                connection.execute(
                    "UPDATE _ledger_entries SET token = ?, expires_at = ? WHERE key = ?",
                    (token, expires_at, key),
                )
            return {"status": "claimed", "token": token, "expires_at": expires_at}

    def renew(
        self, key: str, token: str, *, now_ms: int, lease_ms: int
    ) -> bool:
        """Extend a pending, owned lease without shortening its current expiry.

        Returns False for missing, stale, expired, or already completed claims.
        All input validation happens before opening a write transaction.
        """
        key = _text(key, _KEY, "key")
        token = _text(token, _HEX, "token")
        now_ms = _integer(now_ms, 0, _MAX_TIME, "time")
        lease_ms = _integer(lease_ms, 1, _MAX_LEASE, "lease")
        if now_ms + lease_ms > _MAX_TIME:
            raise LedgerError("Lease expiry exceeds the time limit.")
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE _ledger_entries SET expires_at = max(expires_at, ?) "
                "WHERE key = ? AND token = ? AND state = 'pending' AND expires_at > ?",
                (now_ms + lease_ms, key, token, now_ms),
            )
            return cursor.rowcount == 1

    def complete(self, key: str, token: str, result: object, *, now_ms: int) -> bool:
        """Store a result only for the current token before its lease expires.

        Returns False for missing, stale, expired, or already completed claims.
        Result validation happens before opening a write transaction.
        """
        key = _text(key, _KEY, "key")
        token = _text(token, _HEX, "token")
        now_ms = _integer(now_ms, 0, _MAX_TIME, "time")
        encoded = _encode_result(result)
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE _ledger_entries SET state = 'completed', result = ? "
                "WHERE key = ? AND token = ? AND state = 'pending' AND expires_at > ?",
                (encoded, key, token, now_ms),
            )
            return cursor.rowcount == 1
