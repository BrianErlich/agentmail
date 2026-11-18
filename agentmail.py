#!/usr/bin/env python3
"""agentmail: the record of who asked whom for what, for AI coding-agent sessions on one machine.

Design: DESIGN.md (section numbers below refer to it). Stage 0: the daemon runs as the same user as
its clients. Python standard library only, one file.

    agentmail send TO --kind request|report|info [--subject S] [--re ID [--more]] [--ref PATH]...
                   [--ttl DAYS] [--file BODY] [--request-id ID] [--again]      body from --file or stdin
    agentmail inbox [--all] | read ID | ref-check ID [N] | ack ID... | out
    agentmail close ID [--declined] [--note TEXT] | wait --re ID [--max MINUTES]
    agentmail seats | desk | register NAME | hook session-start|prompt|guard
    agentmail doctor [--accept-clock] | serve
    old names: recv, who, register (with a seat), wait (without --re)

Mail is information, never permission. The daemon decides who a message is from: SO_PEERCRED gives
the caller's pid, and the nearest ancestor whose command line names a seat (Claude Code's
--remote-control NAME; Codex's listening app-server) is the sender. Under one uid that is a
convention, not a proof, and every stamp says so: [conv].

Codex reaches the daemon through a spool folder (AGENTMAIL_TRANSPORT=spool: <home>/spool, seat `codex`).
A roster seat with `engine = "codex"` and `spool = PATH` is a second Codex seat: a request the Codex
app-server writes in PATH (client: AGENTMAIL_SPOOL=PATH) gets that seat. The folder decides, never the caller.
"""
from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import os
import pwd
import re
import select
import selectors
import signal
import socket
import sqlite3
import stat
import struct
import sys
import threading
import time
import tomllib
import uuid
from pathlib import Path

# Configuration comes from the environment only (never from a request). Defaults follow the XDG base directories.
_STATE = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state")
_CONFIG = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
HOME = Path(os.environ.get("AGENTMAIL_HOME") or _STATE / "agentmail")          # store, socket, shared spool
ROSTER = Path(os.environ.get("AGENTMAIL_ROSTER") or _CONFIG / "agentmail" / "roster.toml")   # optional
SETTINGS = Path(os.environ.get("AGENTMAIL_CLAUDE_SETTINGS") or Path.home() / ".claude" / "settings.json")
UNIT = os.environ.get("AGENTMAIL_UNIT") or "agentmail.service"   # what doctor expects the daemon to run under
JOURNAL_PREFIX = "agentmail-event "
NAME_RE = re.compile(r"[a-z][a-z0-9-]{1,31}")
ID_RE = re.compile(r"[0-9a-f]{12}")
RID_RE = re.compile(r"[0-9a-f]{32}")
KINDS = ("request", "report", "info")
CLOSED = ("answered", "declined", "withdrawn", "expired")
STAMP = ("id", "ts", "from", "from_account", "level", "from_kind", "from_pid", "bytes", "sha256")

MIN, HOUR, DAY = 60, 3600, 86400
KIB, MIB = 1024, 1024 * 1024
MAX_LINE, MAX_REQUEST = 4 * KIB, 64 * KIB          # §6.1
MAX_BODY, MAX_SUBJECT, MAX_NOTE = 32 * KIB, 120, 120
MAX_REFS, MAX_REF_LEN = 10, 256
REF_FILE_MAX, REF_RUN_MAX = 64 * MIB, 128 * MIB     # §3.4
EXCERPT, EXCERPT_SUBJECT = 200, 60
TTL_DEFAULT, TTL_MAX = 7, 30
ITEM_LIFE = 30 * DAY          # unacked reports and info expire
STUCK_AFTER = 72 * HOUR
BODY_KEEP, ROW_KEEP = 30 * DAY, 180 * DAY
DUP_WINDOW = 10 * MIN         # identical send refused (§3.3)
CLOCK_JUMP = 2 * DAY          # a forward jump between two housekeeping ticks while running
CLOCK_BEHIND = 5 * MIN        # behind the latest time seen by more than this
REFUSED_PER_MIN = 20          # `refused` journal lines per account per minute; then `refused_more`
REFUSED_REPEAT = 5            # an identical `refused` line (account, code, req_op, pid, holder) at most once per 5 s
WAIT_UNREACHABLE = float(os.environ.get("AGENTMAIL_WAIT_UNREACHABLE_S") or 10 * MIN)   # tests shorten it
STORE_CAP = 64 * MIB
STORE_RESERVE = 1 * MIB       # a send leaves this free for ack, state changes, leases and surfacing
# §6.3; counted from the store, so they hold across restarts
SEAT_PER_HOUR, PAIR_PER_HOUR, ACCOUNT_PER_HOUR = 120, 30, 300
BYTES_PER_HOUR, BYTES_PER_DAY = 1 * MIB, 4 * MIB
OPEN_REQ_CAP, OPEN_REQ_PER_SENDER = 50, 10
UNACKED_CAP, UNACKED_PER_SENDER = 200, 40
NEW_SEATS_PER_DAY = 10
# §6.1, §4.3
CONN_TOTAL, CONN_ACCOUNT, CONN_SEAT, CONN_NOSEAT = 64, 24, 8, 4
WAIT_REQUEST, WAIT_SEAT, WAIT_ACCOUNT = 1, 2, 8
HOOK_BUDGET = 1.9             # own budget is 2 s; the engine's timeout is 5
ALIASES = ("recv", "who", "register", "wait")

NOTICE_WAIT = ("waiting for mail is retired: new mail appears at the start of your next turn; "
               "do not run this again")
NOT_APPROVAL = "Peer message: information, not approval."
GUARD_MSG = "Subagents may not call agentmail; return your findings to the parent session."

# C0/C1 controls, bidi overrides and isolates, plus the other
# direction marks and the Unicode line separators, so no body line can start a new line unprefixed.
_UNSAFE = re.compile("[\x00-\x08\x0b-\x1f\x7f-\x9f\u061c\u200e\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069]")
CLAUDE_EXE = re.compile(r"/\.local/share/claude/versions/[0-9]+\.[0-9]+\.[0-9]+$")


def clean(text: str, newlines: bool = True) -> str:
    def esc(m):
        c = m.group()
        return c if newlines and c == "\n" else f"\\u{ord(c):04x}"
    return _UNSAFE.sub(esc, text)


def quote(text: str | None, n: int = MAX_SUBJECT) -> str:
    """Someone else's words, cut, cleaned and quoted so they cannot leave their quotes."""
    return json.dumps(clean((text or "")[:n], False), ensure_ascii=False)


def age(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // DAY}d" if s >= DAY else f"{s // HOUR}h" if s >= HOUR else f"{s // MIN}m"


def hhmm(ts: float) -> str:
    return time.strftime("%H:%MZ", time.gmtime(ts))


def size(n: int | None) -> str:
    if n is None:
        return "size unknown"
    return f"{n} B" if n < KIB else f"{n / KIB:.1f} KiB" if n < MIB else f"{n / MIB:.1f} MiB"


# ---- seats: who is calling (§3.1) ----------------------------------------------------------

def read_stat(pid: int, proc: str = "/proc") -> tuple[int, int] | None:
    """(ppid, starttime) from /proc/<pid>/stat; the comm field may contain spaces and parens."""
    try:
        raw = Path(f"{proc}/{pid}/stat").read_text()
        fields = raw[raw.rindex(")") + 2:].split()
        return int(fields[1]), int(fields[19])
    except (OSError, ValueError, IndexError):
        return None


def read_cmdline(pid: int, proc: str = "/proc") -> list[str]:
    """argv, split at NUL bytes. Empty arguments are kept (`--tools '' --remote-control x`
    is three arguments before the name, and dropping the empty one misreads the next)."""
    try:
        raw = Path(f"{proc}/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    if not raw:
        return []
    if raw.endswith(b"\0"):
        raw = raw[:-1]
    return [a.decode("utf-8", "replace") for a in raw.split(b"\0")]


def _fit(name: str | None) -> str | None:
    name = (name or "").lower()
    return name if NAME_RE.fullmatch(name) else None


def engine_rule(argv: list[str]) -> tuple[str, str | None] | None:
    """(engine, seat name or None) when argv[0] is a known engine; None for anything else."""
    if not argv:
        return None
    base = os.path.basename(argv[0])
    if base == "claude":
        for i, a in enumerate(argv[1:], 1):
            if a == "--remote-control":
                return "claude", _fit(argv[i + 1] if i + 1 < len(argv) else None)
            if a.startswith("--remote-control="):
                return "claude", _fit(a.split("=", 1)[1])
        return "claude", None
    if base == "codex":
        # Only the app-server that listens is the Codex seat; its launcher, proxy and helpers are not.
        listens = len(argv) > 1 and argv[1] == "app-server" and any(
            a == "--listen" or a.startswith("--listen=") for a in argv[2:])
        return "codex", "codex" if listens else None
    return None


def exe_matches(kind: str, pid: int, proc: str = "/proc") -> bool:
    """Where /proc/<pid>/exe is readable (stage 0, same uid), the engine binary must match too."""
    try:
        exe = os.readlink(f"{proc}/{pid}/exe")
    except OSError:
        return True   # unreadable (another uid, stage 1): the check is cmdline only
    exe = exe.removesuffix(" (deleted)")   # an engine upgraded under a running session
    if kind == "claude":
        return bool(CLAUDE_EXE.search(exe)) or exe.endswith("/claude")
    return exe.endswith("/codex")


class Resolver:
    """Walks parent pids from the caller to the nearest engine process that names a seat.
    Test policy only: `check_exe=False` relaxes the executable check (stub engines are python),
    and `stop_at` ends the walk at that pid (so a test runner's own session is never reached)."""

    def __init__(self, proc: str = "/proc", check_exe: bool = True, stop_at: int | None = None):
        self.proc, self.check_exe, self.stop_at = proc, check_exe, stop_at

    def engine_at(self, pid: int) -> tuple[str, str | None] | None:
        rule = engine_rule(read_cmdline(pid, self.proc))
        if rule and self.check_exe and not exe_matches(rule[0], pid, self.proc):
            return None   # looks like an engine, is not one: transparent
        return rule

    def __call__(self, pid: int) -> dict:
        p, hops, top = pid, 0, None
        while p > 1 and hops < 64 and p != self.stop_at:
            st = read_stat(p, self.proc)
            if st is None:
                break
            rule = self.engine_at(p)
            if rule:
                if rule[1]:
                    return {"kind": rule[0], "pid": p, "start": st[1], "name": rule[1]}
                top = {"kind": rule[0], "pid": p, "start": st[1], "name": None}   # skipped, kept for register
            p, hops = st[0], hops + 1
        if top:
            return top
        st = read_stat(pid, self.proc)
        return {"kind": None, "pid": pid, "start": st[1] if st else 0, "name": None}

    def engines(self):
        """(pid, kind, name) of every engine process that names a seat (for doctor)."""
        for e in os.scandir(self.proc):
            if e.name.isdigit():
                rule = self.engine_at(int(e.name))
                if rule and rule[1]:
                    yield int(e.name), rule[0], rule[1]


def alive(ident: dict) -> bool:
    st = read_stat(ident["pid"])
    return st is not None and st[1] == ident["start"]


def nearest_engine(pid: int) -> int | None:
    """Nearest engine ancestor, named or not, by command line only (bare `wait`; no daemon)."""
    p, hops = pid, 0
    while p > 1 and hops < 64:
        st = read_stat(p)
        if st is None:
            return None
        if engine_rule(read_cmdline(p)):
            return p
        p, hops = st[0], hops + 1
    return None


# ---- store (§6.2) --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS msg(id TEXT PRIMARY KEY, ts REAL, from_seat TEXT, from_account INTEGER, level TEXT,
  from_kind TEXT, from_pid INTEGER, to_seat TEXT, to_account INTEGER, kind TEXT, re TEXT, subject TEXT,
  body TEXT, refs TEXT, bytes INTEGER, sha256 TEXT, ttl_ts REAL, state TEXT, state_ts REAL, note TEXT,
  surfaced_ts REAL, seen_ts REAL, acked_ts REAL, notified_ts REAL, stuck_ts REAL,
  stuck_notified_ts REAL, subject_given INTEGER DEFAULT 0, answer_id TEXT);
CREATE INDEX IF NOT EXISTS msg_to ON msg(to_seat, state);
CREATE INDEX IF NOT EXISTS msg_from ON msg(from_seat, ts);
CREATE INDEX IF NOT EXISTS msg_re ON msg(re);
CREATE TABLE IF NOT EXISTS seat(name TEXT PRIMARY KEY, account INTEGER, first_ts REAL, last_seen_ts REAL,
  desk_shown TEXT, first_pid INTEGER);
CREATE TABLE IF NOT EXISTS lease(seat TEXT PRIMARY KEY, account INTEGER, pid INTEGER, start INTEGER, since REAL,
  kind TEXT);
CREATE TABLE IF NOT EXISTS req(account INTEGER, seat TEXT, request_id TEXT, request_sha256 TEXT, result TEXT,
  ts REAL, msg_id TEXT, PRIMARY KEY(account, seat, request_id));
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS conflict(seat TEXT, holder_pid INTEGER, claimant_pid INTEGER, ts REAL,
  PRIMARY KEY(seat, holder_pid, claimant_pid));
"""


def open_store(path: Path) -> sqlite3.Connection:
    old = os.umask(0o077)
    try:
        db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
    finally:
        os.umask(old)
    db.row_factory = sqlite3.Row
    # A rollback journal with EXTRA keeps the last commit through a power cut (sqlite.org).
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=EXTRA")
    db.executescript(SCHEMA)
    # The hard backstop under the send-side check: SQLite itself refuses to grow the file past the cap.
    # STORE_CAP is read at open, so a test that patches it opens its store afterwards.
    page = db.execute("PRAGMA page_size").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={max(1, STORE_CAP // page)}")
    return db


def store_full(e: BaseException) -> bool:
    """SQLITE_FULL: the max_page_count backstop, or the disk itself."""
    return isinstance(e, sqlite3.OperationalError) and (
        getattr(e, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL or "database or disk is full" in str(e))


def store_ok(rep: dict) -> bool:
    return rep["store_bytes"] <= rep["store_cap"]


def lock_home(home: Path):
    """The one-writer lock: only one daemon serves a home."""
    f = os.fdopen(os.open(home / "daemon.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600), "r+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        return None
    return f


# ---- roster (§8) ---------------------------------------------------------------------------

ROSTER_KEYS = {"overview": bool, "reads": str, "names": list, "where": str, "retired": bool, "account": int,
               "engine": str, "spool": str}
SPOOL_SEAT_KEYS = {"engine", "spool", "overview", "reads", "retired"}


def _inside(path: str, top: str) -> bool:
    return path == top or path.startswith(top.rstrip("/") + "/")


def _spool_key(name: str, cfg: dict, spools: dict):
    """A seat spool: a Codex seat chosen by the folder its request is written in."""
    if cfg.get("engine", "codex") != "codex":
        raise ValueError(f"seat {name}: engine must be \"codex\"")
    if "spool" not in cfg:
        return
    if name == "codex":
        raise ValueError("seat codex: its spool is the shared one; it takes no spool key")
    if cfg.get("engine") != "codex":
        raise ValueError(f"seat {name}: spool needs engine = \"codex\"")
    if set(cfg) - SPOOL_SEAT_KEYS:
        raise ValueError(f"seat {name}: a spool seat takes only {', '.join(sorted(SPOOL_SEAT_KEYS))}")
    p = cfg["spool"]
    if (not p.startswith("/") or p.startswith("//") or p == "/" or len(p) > 1024 or "\0" in p
            or _UNSAFE.search(p) or os.path.normpath(p) != p):
        raise ValueError(f"seat {name}: spool must be an absolute, normalized path")
    if p in spools:
        raise ValueError(f"spool {p} named twice ({spools[p]}, {name})")
    spools[p] = name


def check_spools(seats: dict, home: Path) -> dict:
    """No seat spool is the daemon home's shared spool or lies inside it (by name or by its real path)."""
    shared = Path(home) / "spool"
    tops = {os.path.normpath(os.path.abspath(shared)), os.path.realpath(shared)}
    for name, cfg in seats.items():
        for top in tops:
            if "spool" in cfg and _inside(cfg["spool"], top):
                raise ValueError(f"seat {name}: spool may not be or lie inside the shared spool {top}")
    return seats


def parse_roster(text: str, home: Path | None = None) -> dict:
    """`home`: the daemon's home, so a seat spool cannot be the shared spool or lie inside it."""
    if home is not None:
        return check_spools(parse_roster(text), home)
    data = tomllib.loads(text)
    if set(data) - {"seat"}:
        raise ValueError(f"unknown top-level keys: {sorted(set(data) - {'seat'})}")
    seats, claimed, spools = data.get("seat", {}), {}, {}
    if not isinstance(seats, dict):
        raise ValueError("[seat.NAME] tables expected")
    for name, cfg in seats.items():
        if not NAME_RE.fullmatch(name) or not isinstance(cfg, dict):
            raise ValueError(f"bad seat {name!r}")
        for k, v in cfg.items():
            if k not in ROSTER_KEYS or type(v) is not ROSTER_KEYS[k]:
                raise ValueError(f"seat {name}: bad key or value {k!r}")
        if "reads" in cfg and cfg["reads"] != "on request":
            raise ValueError(f"seat {name}: reads must be \"on request\"")
        if "where" in cfg and not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", cfg["where"]):
            raise ValueError(f"seat {name}: bad where")
        _spool_key(name, cfg, spools)
        for n in [name, *cfg.get("names", [])]:
            if not isinstance(n, str) or not NAME_RE.fullmatch(n):
                raise ValueError(f"seat {name}: bad name {n!r}")
            if n in claimed:
                raise ValueError(f"name {n} claimed twice ({claimed[n]}, {name})")
            claimed[n] = name
    return seats


# ---- the mailbox ---------------------------------------------------------------------------

class Refuse(Exception):
    """`what` is a fixed word for the journal only (no_seat, recipient, stamp_field, unknown_op,
    unknown_field, roster, conn_cap, frame, uid, other); replies never carry it."""
    def __init__(self, code: str, reason: str, what: str = "other", **extra):
        super().__init__(reason)
        self.code, self.reason, self.what, self.extra = code, reason, what, extra

    def reply(self) -> dict:
        return {"ok": False, "code": self.code, "error": self.reason, **self.extra}


# Keys each op accepts. Anything else, and any stamp field above all, is E_FORMAT.
OPS = {
    "send": {"to", "kind", "subject", "re", "more", "refs", "ttl_days", "request_id", "again"},
    "inbox": {"all"}, "read": {"msg"}, "refs": {"msg"}, "ack": {"msgs", "request_id"},
    "out": set(), "close": {"msg", "declined", "note", "request_id"}, "wait": {"msg"},
    "seats": set(), "desk": set(), "register": {"name"}, "hook": {"event"},
    "doctor": set(), "accept_clock": set(),
}
NO_SEAT_OK = {"seats", "doctor", "register", "hook", "accept_clock"}


class Mailbox:
    def __init__(self, home: Path, resolver=None, is_alive=alive, clock=time.time, journal=None,
                 roster: Path | None = None, uid: int | None = None, unit=None):
        self.home = Path(home)
        self.home.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.db = open_store(self.home / "store.db")
        self.resolver = resolver or Resolver()
        self.is_alive, self.clock, self.journal = is_alive, clock, journal
        self.roster_path = Path(roster) if roster else ROSTER
        self.uid = os.getuid() if uid is None else uid
        self.accounts = {self.uid}            # stage 0: the daemon's own account only
        self.unit = unit or own_unit
        self.watch = None                     # set by the server: pidfd per lease
        self.waits: dict = {}                 # waiter -> (msg id, seat, account)
        self.ready: list = []                 # (waiter, reply) for the server to deliver
        self.fail_after_commit = False        # tests: a crash between commit and reply
        cm = self._meta("clock_max")
        if cm is None:
            cm = self.db.execute("SELECT max(ts) FROM msg").fetchone()[0]
        self.clock_max = float(cm) if cm is not None else None
        self._clock_saved, self.clock_fault, self._jumped = self.clock_max, None, False
        self._refused: dict = {}              # account -> [minute, lines this minute, suppressed, {code: n}]
        self._refused_seen: dict = {}         # (account, code, req_op, pid, holder) -> clock of the last line
        self._cmd_alias_seen: dict = {}       # (seat, retired command) -> clock of the last journal line
        self._acct_names: dict = {}           # uid -> user name, for journal lines
        # At start, a forward gap of any size is downtime, not a fault; behind is still a fault.
        self.last_tick = self._now()
        good = self._meta("roster_good")
        self.roster = json.loads(good) if good else {}
        self.roster_gen = 0                   # bumped on every roster change (the server re-reads seat spools)
        self.roster_err, self.roster_mtime = None, None
        self.roster_check()

    # helpers
    def _meta(self, key):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    def _set_meta(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))

    @contextlib.contextmanager
    def tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
            if self.clock_max is not None and self.clock_max != self._clock_saved:
                self._set_meta("clock_max", repr(self.clock_max))
            self.db.execute("COMMIT")
            self._clock_saved = self.clock_max
        except BaseException:
            if self.db.in_transaction:      # SQLITE_FULL may already have rolled it back
                self.db.execute("ROLLBACK")
            raise

    def _acct_name(self, uid: int) -> str:
        if uid not in self._acct_names:
            try:
                self._acct_names[uid] = pwd.getpwuid(uid).pw_name
            except (KeyError, OverflowError):
                self._acct_names[uid] = str(uid)
        return self._acct_names[uid]

    def _log(self, op: str, **fields):
        if self.journal:
            # Log consumers can rely on `account` being a string (user name) and `uid` an int.
            if isinstance(fields.get("account"), int) and not isinstance(fields["account"], bool):
                fields["uid"] = fields["account"]
                fields["account"] = self._acct_name(fields["uid"])
            # One line per event on stdout (the journal under systemd); best effort, the store is the record.
            self.journal(JOURNAL_PREFIX + json.dumps({"ts": self.clock(), "op": op, **fields}))

    def _now(self) -> float:
        """§6.2: more than 5 minutes behind the latest time seen is a fault,
        at start or while running; a forward jump of over 2 days between ticks is one (see tick)."""
        t = self.clock()
        m = self.clock_max
        if m is not None and t < m - CLOCK_BEHIND:
            self.clock_fault = "behind"
            return t
        if m is None or t > m:
            self.clock_max = t
        self.clock_fault = "ahead" if self._jumped else None
        return t

    def clock_state(self) -> str:
        if not self.clock_fault:
            return "ok"
        what = ("clock behind the latest time seen by more than 5 minutes" if self.clock_fault == "behind"
                else "clock jumped ahead more than 2 days between two checks")
        return (f"{what} ({time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime(self.clock_max))}): nothing expires "
                f"or is deleted until it is back in range or the store's owner runs doctor --accept-clock")

    def roster_check(self):
        """Reload when the file's mtime changes; keep and store the last good copy on failure."""
        try:
            mt = self.roster_path.stat().st_mtime_ns
        except OSError:
            mt = None
        if mt == self.roster_mtime:
            return
        self.roster_mtime = mt
        try:
            new = check_spools(parse_roster(self.roster_path.read_text()), self.home) if mt is not None else {}
        except (OSError, ValueError, tomllib.TOMLDecodeError) as e:
            self.roster_err = clean(str(e), False)[:200]
            self.log_refused(self.uid, code="E_FORMAT", what="roster", error=self.roster_err)
            return
        self.roster_err = None
        if new != self.roster or self._meta("roster_good") is None:
            self.roster = new
            self.roster_gen += 1
            with self.tx():
                self._set_meta("roster_good", json.dumps(new))

    def alias_of(self, name: str) -> str:
        for seat, cfg in self.roster.items():
            if name in cfg.get("names", []):
                return seat
        return name

    def _lease(self, seat):
        return self.db.execute("SELECT * FROM lease WHERE seat=?", (seat,)).fetchone()

    @staticmethod
    def _lident(row) -> dict:
        return {"kind": row["kind"], "pid": row["pid"], "start": row["start"]}

    def _get(self, msg_id):
        if not isinstance(msg_id, str) or not ID_RE.fullmatch(msg_id):
            raise Refuse("E_FORMAT", "a message id is 12 hex characters")
        m = self.db.execute("SELECT * FROM msg WHERE id=?", (msg_id,)).fetchone()
        if m is None:
            raise Refuse("E_NOTFOUND", f"no message {msg_id}")
        return m

    def running(self, seat) -> bool:
        lease = self._lease(seat)
        return bool(lease) and self.is_alive(self._lident(lease))

    def seat_key(self, ident: dict) -> str | None:
        """The seat a connection counts against, without creating anything (connection caps)."""
        if not ident.get("kind"):
            return None
        if ident.get("name"):
            return self.alias_of(ident["name"])
        r = self.db.execute("SELECT seat FROM lease WHERE pid=? AND start=?", (ident["pid"], ident["start"])).fetchone()
        return r[0] if r else None

    # leases
    def _end_lease(self, seat, row, now):
        with self.tx():
            self.db.execute("DELETE FROM lease WHERE seat=? AND pid=? AND start=?", (seat, row["pid"], row["start"]))
            self.db.execute("UPDATE seat SET last_seen_ts=? WHERE name=?", (now, seat))
        self._log("lease_end", name=seat, kind=row["kind"], pid=row["pid"], start=row["start"])
        for waiter, (_, s, *_) in list(self.waits.items()):
            if s == seat:
                del self.waits[waiter]
                self.ready.append((waiter, {"ok": False, "code": "E_SEAT", "error": "the session ended"}))

    def lease_gone(self, seat, ident):
        """The server's pidfd for a lease became readable: that engine process has exited."""
        row = self._lease(seat)
        if row and (row["pid"], row["start"]) == (ident["pid"], ident["start"]):
            self._end_lease(seat, row, self._now())

    def rewatch(self):
        for row in self.db.execute("SELECT * FROM lease").fetchall():
            ident = self._lident(row)
            if not (self.is_alive(ident) and self._watch(row["seat"], ident)):
                self._end_lease(row["seat"], row, self._now())

    def _watch(self, seat, ident) -> bool:
        return True if self.watch is None else self.watch(seat, ident)

    def _new_lease(self, seat, ident, account, now):
        r = self.roster.get(seat, {})
        if r.get("retired"):
            raise Refuse("E_SEAT", f"{seat} is retired")
        if r.get("where"):
            raise Refuse("E_SEAT", f"{seat} is on {r['where']}, not on this machine")
        if "account" in r and r["account"] != account:
            raise Refuse("E_SEAT", f"{seat} belongs to account {r['account']}")
        known = self.db.execute("SELECT 1 FROM seat WHERE name=?", (seat,)).fetchone()
        if not known and seat not in self.roster:
            rows = self.db.execute("SELECT name FROM seat WHERE account=? AND first_ts>? AND first_pid IS NOT NULL",
                                   (account, now - DAY)).fetchall()
            if sum(1 for x in rows if x[0] not in self.roster) >= NEW_SEATS_PER_DAY:
                raise Refuse("E_LIMIT", f"{NEW_SEATS_PER_DAY} new seat names a day per account")
        if not self._watch(seat, ident):
            raise Refuse("E_SEAT", "the engine process has exited")
        with self.tx():
            self.db.execute("INSERT OR REPLACE INTO lease VALUES(?,?,?,?,?,?)",
                            (seat, account, ident["pid"], ident["start"], now, ident["kind"]))
            if known:
                self.db.execute("UPDATE seat SET last_seen_ts=?, account=?, first_pid=coalesce(first_pid, ?) "
                                "WHERE name=?", (now, account, ident["pid"], seat))
            else:
                self.db.execute("INSERT INTO seat(name, account, first_ts, last_seen_ts, first_pid) VALUES(?,?,?,?,?)",
                                (seat, account, now, now, ident["pid"]))
        spool = {"spool_seat": True} if r.get("spool") else {}
        self._log("register", name=seat, kind=ident["kind"], pid=ident["pid"], start=ident["start"],
                  account=account, level=self.level(account), **spool)
        if not known:
            self._log("seat_new", name=seat, kind=ident["kind"], pid=ident["pid"], account=account,
                      in_roster=seat in self.roster, **spool)

    def level(self, account) -> str:
        # The daemon runs as the callers' own account at stage 0: nothing is proven.
        return "conv" if account == self.uid else "acct"

    def _caller(self, ident, account, op, now) -> dict:
        c = {"ident": ident, "account": account, "seat": None, "kind": ident.get("kind"),
             "pid": ident["pid"], "start": ident["start"], "nameless": False}
        if not ident.get("kind"):
            return c
        if ident.get("name"):
            seat = self.alias_of(ident["name"])
        else:
            seat = self.seat_key(ident)
            if not seat:
                c["nameless"] = True
                return c
        lease = self._lease(seat)
        if self.roster.get(seat, {}).get("spool"):
            if ident.get("spool_seat") != seat:
                raise Refuse("E_SEAT", f"seat {seat} is reached only through its own spool, by the Codex app-server",
                             what="spool_seat", seat=seat)
            if lease and lease["kind"] != "codex":   # leased under that name before the roster made it a spool seat
                self._end_lease(seat, lease, now)
                lease = None
        if lease and (lease["pid"], lease["start"]) == (ident["pid"], ident["start"]):
            c["seat"] = seat
            return c
        if lease and self.is_alive(self._lident(lease)):
            if op == "doctor":
                return c
            if not self.db.execute("SELECT 1 FROM conflict WHERE seat=? AND holder_pid=? AND claimant_pid=?",
                                   (seat, lease["pid"], ident["pid"])).fetchone():
                with self.tx():
                    self.db.execute("INSERT INTO conflict VALUES(?,?,?,?)", (seat, lease["pid"], ident["pid"], now))
            raise Refuse("E_HELD", f"seat {seat} is held by pid {lease['pid']}", seat=seat, holder=lease["pid"])
        if lease:
            self._end_lease(seat, lease, now)
        try:
            self._new_lease(seat, ident, account, now)
        except Refuse:
            if op in NO_SEAT_OK:
                return c
            raise
        c["seat"] = seat
        return c

    # dispatch
    def handle(self, peer_pid: int, peer_uid: int, req, body: bytes | None = None, ident=None, waiter=None,
               spool: tuple[str, str] | None = None) -> dict:
        """`spool`: (seat, path) when the server found this request in that seat's spool folder. The seat
        comes from where the file was written, never from the request; the caller must still be Codex."""
        op = req.get("op") if isinstance(req, dict) else None
        caller = None
        try:
            try:
                if peer_uid not in self.accounts:
                    raise Refuse("E_OWNER", "account not allowed", what="uid")
                if not isinstance(op, str) or op not in OPS:
                    raise Refuse("E_FORMAT", "unknown op", what="unknown_op")
                stamped = sorted(set(req) & set(STAMP))
                if stamped:
                    raise Refuse("E_FORMAT", f"a request cannot supply stamp fields: {', '.join(stamped)}",
                                 what="stamp_field")
                unknown = sorted(set(req) - OPS[op] - {"op", "alias"})
                if unknown:
                    raise Refuse("E_FORMAT", f"unknown fields for {op}: {', '.join(unknown)}", what="unknown_field")
                if "alias" in req and req["alias"] not in ALIASES:
                    raise Refuse("E_FORMAT", "unknown alias", what="unknown_field")
                now = self._now()
                self.roster_check()
                ident = ident or self.resolver(peer_pid)
                if spool:
                    ident = self._spool_ident(ident, *spool)
                caller = self._caller(ident, peer_uid, op, now)
                if op == "hook" and caller["seat"] is None:
                    return {"ok": True, "block": ""}
                if op not in NO_SEAT_OK and caller["seat"] is None:
                    raise Refuse("E_SEAT", "this process has no seat: start the chat with --remote-control NAME, "
                                           "or run agentmail register NAME", what="no_seat")
                if req.get("alias") in ("recv", "who"):
                    self._cmd_alias(req["alias"], caller, now)
                return getattr(self, "op_" + op)(caller, req, body, now, waiter)
            except sqlite3.OperationalError as e:
                if not store_full(e):
                    raise
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                raise Refuse("E_FULL", "the store is at its 64 MiB cap") from None
        except Refuse as e:
            if e.code == "E_LIMIT" and op == "send" and e.extra.pop("rate", False):
                c = caller or {}
                to = req.get("to")
                self._log("rate_limited", **{"from": c.get("seat"),
                                             "to": clean(to, False)[:32] if isinstance(to, str) else to,
                                             "kind": c.get("kind"),
                                             "pid": c.get("pid"), "start": c.get("start"),
                                             "from_kind": c.get("kind"), "account": peer_uid,
                                             "limit": e.reason})
            else:
                i = ident if isinstance(ident, dict) else {}
                self.log_refused(peer_uid, code=e.code, what=e.what,
                          req_op=op if isinstance(op, str) and op in OPS else "?",
                          name=e.extra.get("seat") or i.get("name"), kind=i.get("kind"), pid=i.get("pid", peer_pid),
                          holder=e.extra.get("holder"))
            return e.reply()

    def _spool_ident(self, ident, seat, path) -> dict:
        """A request read from seat `seat`'s spool: only the listening Codex app-server (the engine that is
        seat `codex` elsewhere) gets that seat; anything else is refused, whatever op it asked for."""
        if self.roster.get(seat, {}).get("spool") != path:
            raise Refuse("E_SEAT", f"{path} is no longer seat {seat}'s spool", what="spool_seat", seat=seat)
        if ident.get("kind") != "codex" or ident.get("name") != "codex":
            raise Refuse("E_SEAT", f"this process has no seat: a request in seat {seat}'s spool must come from "
                                   "the Codex app-server", what="spool_seat", seat=seat)
        return {**ident, "name": seat, "spool_seat": seat}

    def log_refused(self, account, key_extra: tuple = (), **fields):
        """At most 20 `refused` lines per account a minute; then one `refused_more` with the count.
        An identical line (account, code, req_op, pid, holder) inside 5 s is not written and not counted."""
        now = self.clock()
        key = (account, fields.get("code"), fields.get("req_op"), fields.get("pid"), fields.get("holder"), *key_extra)
        last = self._refused_seen.get(key)
        if last is not None and 0 <= now - last < REFUSED_REPEAT:
            return
        if len(self._refused_seen) > 1024:
            self._refused_seen = {k: t for k, t in self._refused_seen.items() if 0 <= now - t < REFUSED_REPEAT}
        self._refused_flush(now)
        w = self._refused.setdefault(account, [int(now // MIN), 0, 0, {}])
        if w[1] < REFUSED_PER_MIN:
            w[1] += 1
            self._refused_seen[key] = now
            self._log("refused", account=account, **fields)
        else:
            w[2] += 1
            code = str(fields.get("code"))
            w[3][code] = w[3].get(code, 0) + 1

    def flush_refused(self):
        """The daemon is stopping: write every `refused_more` still held, whatever the minute."""
        self._refused_flush(self.clock(), final=True)

    def _refused_flush(self, now, final=False):
        """A minute that has ended (or, with final, every minute: the daemon is stopping)."""
        minute = int(now // MIN)
        for account, w in list(self._refused.items()):
            if final or w[0] != minute:
                if w[2]:
                    self._log("refused_more", account=account, n=w[2], minute=w[0] * MIN, by_code=dict(w[3]))
                del self._refused[account]

    def _cmd_alias(self, cmd, caller, now):
        """A retired command (recv, who, register, wait) was used: one journal line per seat and command an hour."""
        key = (caller["seat"], cmd)
        last = self._cmd_alias_seen.get(key)
        if last is None or not 0 <= now - last < HOUR:
            self._cmd_alias_seen[key] = now
            seat = {"name": caller["seat"]} if caller["seat"] else {}
            self._log("cmd_alias", cmd=cmd, **seat, kind=caller["kind"], pid=caller["pid"], account=caller["account"])
        last = self._meta(f"alias:{cmd}")
        if last is None or now - float(last) > HOUR:   # at most one write an hour per old name
            with self.tx():
                self._set_meta(f"alias:{cmd}", repr(now))

    # retries (§3.3)
    def _retry(self, caller, req, body):
        rid = req.get("request_id")
        if rid is None:
            return None, None
        if not isinstance(rid, str) or not RID_RE.fullmatch(rid):
            raise Refuse("E_FORMAT", "request_id is 32 hex characters")
        h = hashlib.sha256(json.dumps({k: v for k, v in req.items() if k not in ("request_id", "alias")},
                                      sort_keys=True).encode() + b"\0" + (body or b"")).hexdigest()
        row = self.db.execute("SELECT * FROM req WHERE account=? AND seat=? AND request_id=?",
                              (caller["account"], caller["seat"], rid)).fetchone()
        if row:
            if row["request_sha256"] != h:
                raise Refuse("E_DUP", f"request id {rid} was used for a different request")
            return json.loads(row["result"]), None
        return None, (rid, h)

    def _keep(self, caller, key, result, msg_id, now):
        if key:
            self.db.execute("INSERT INTO req VALUES(?,?,?,?,?,?,?)",
                            (caller["account"], caller["seat"], key[0], key[1], json.dumps(result), now, msg_id))

    # send (§3.2)
    def op_send(self, c, req, body, now, waiter):
        to, kind, subject, re_id = req.get("to"), req.get("kind"), req.get("subject"), req.get("re")
        refs, ttl, more = req.get("refs", []), req.get("ttl_days"), req.get("more", False)
        if kind not in KINDS:
            raise Refuse("E_KIND", "kind must be request, report or info (no message is an approval)")
        if body is None:
            raise Refuse("E_FORMAT", "send carries its body as a frame")
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            raise Refuse("E_FORMAT", "the body is not UTF-8") from None
        if not text.strip() or len(body) > MAX_BODY:
            raise Refuse("E_SIZE", f"body must be 1..{MAX_BODY} bytes")
        if subject is not None and (not isinstance(subject, str) or "\n" in subject or "\r" in subject):
            raise Refuse("E_FORMAT", "the subject is one line of text")
        if subject is not None and (len(subject) > MAX_SUBJECT or not subject.strip()):
            raise Refuse("E_SIZE", f"subject: 1..{MAX_SUBJECT} characters")
        if (not isinstance(refs, list) or len(refs) > MAX_REFS
                or not all(isinstance(r, dict) and set(r) <= {"path", "sha256", "size"}
                           and isinstance(r.get("path"), str) and 0 < len(r["path"]) <= MAX_REF_LEN
                           and (r.get("sha256") is None or (isinstance(r["sha256"], str)
                                                            and re.fullmatch(r"[0-9a-f]{64}", r["sha256"])))
                           and (r.get("size") is None or (isinstance(r["size"], int) and r["size"] >= 0))
                           for r in refs)):
            raise Refuse("E_SIZE", f"refs: at most {MAX_REFS} paths of {MAX_REF_LEN} characters")
        if ttl is not None and (kind != "request" or type(ttl) is not int or not 1 <= ttl <= TTL_MAX):
            raise Refuse("E_FORMAT", f"--ttl is for requests only, 1 to {TTL_MAX} days")
        if more and not re_id:
            raise Refuse("E_FORMAT", "--more needs --re")
        if re_id is not None and kind != "report":
            raise Refuse("E_KIND", "--re is valid only on a report")
        if not isinstance(to, str) or not _fit(to):
            raise Refuse("E_SEAT", "a seat name matches [a-z][a-z0-9-]{1,31}", what="recipient")
        to = self.alias_of(to.lower())
        done, key = self._retry(c, req, body)
        if done:
            return done
        self._check_recipient(c, to)
        target = None
        if re_id is not None:
            target = self._get(re_id)
            if target["kind"] != "request":
                raise Refuse("E_KIND", f"{re_id} is a {target['kind']}, not a request")
            if target["to_seat"] != c["seat"] or target["from_seat"] != to:
                raise Refuse("E_PARTY", "--re goes only on a report from a request's recipient back to its sender")
            if target["state"] != "open":
                raise Refuse("E_STATE", f"request {re_id} is {target['state']}")
        if subject is None:
            first = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
            subj, given = first[:MAX_SUBJECT], 0
        else:
            subj, given = subject, 1
        digest = hashlib.sha256(body).hexdigest()
        if not req.get("again"):
            prev = self.db.execute(
                "SELECT id, ts FROM msg WHERE from_seat=? AND to_seat=? AND kind=? AND re IS ? AND subject=? AND sha256=? "
                "AND ts>? ORDER BY ts LIMIT 1",
                (c["seat"], to, kind, re_id, subj, digest, now - DUP_WINDOW)).fetchone()
            if prev:
                result = {"ok": True, "id": prev["id"], "to": to, "already": True,
                          "min_ago": int((now - prev["ts"]) // MIN)}
                if key:
                    with self.tx():
                        self._keep(c, key, result, prev["id"], now)
                return result
        self._limits(c, to, kind, len(body), now)
        if (self.home / "store.db").stat().st_size + 2 * len(body) + STORE_RESERVE > STORE_CAP:
            raise Refuse("E_FULL", "the store is at its 64 MiB cap")
        msg_id = uuid.uuid4().hex[:12]
        while self.db.execute("SELECT 1 FROM msg WHERE id=?", (msg_id,)).fetchone():
            msg_id = uuid.uuid4().hex[:12]
        to_row = self.db.execute("SELECT account FROM seat WHERE name=?", (to,)).fetchone()
        to_account = to_row[0] if to_row and to_row[0] is not None else self.roster.get(to, {}).get("account", self.uid)
        ttl_ts = now + (ttl or TTL_DEFAULT) * DAY if kind == "request" else now + ITEM_LIFE
        result = {"ok": True, "id": msg_id, "to": to, "note": self._delivery(to, now)}
        with self.tx():
            self.db.execute(
                "INSERT INTO msg(id, ts, from_seat, from_account, level, from_kind, from_pid, to_seat, to_account, kind, "
                "re, subject, body, refs, bytes, sha256, ttl_ts, state, state_ts, subject_given) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (msg_id, now, c["seat"], c["account"], self.level(c["account"]), c["kind"], c["pid"], to, to_account,
                 kind, re_id, subj, text, json.dumps(refs), len(body), digest, ttl_ts, "open", now, given))
            if target is not None and not more:
                self.db.execute("UPDATE msg SET state='answered', state_ts=?, answer_id=?, notified_ts=? WHERE id=?",
                                (now, msg_id, now, re_id))
            self._keep(c, key, result, msg_id, now)
        excerpt = (clean(subj[:EXCERPT_SUBJECT], False) + " | " + text) if given else text
        subj_fields = ({"subject_n": len(subj), "subject_sha256": hashlib.sha256(subj.encode()).hexdigest()}
                       if given else {})
        self._log("send", id=msg_id, **{"from": c["seat"]}, from_kind=c["kind"], from_pid=c["pid"], to=to, kind=kind,
                  bytes=len(body), sha256=digest, refs_n=len(refs), excerpt=clean(excerpt[:EXCERPT], False),
                  level=self.level(c["account"]), account=c["account"], re=re_id,
                  to_account=self._acct_name(to_account), to_uid=to_account, **subj_fields)
        if target is not None and not more:
            self._log("state", id=re_id, state="answered", name=c["seat"], answer=msg_id)
            self._wake(re_id)
        if self.fail_after_commit:
            self.fail_after_commit = False
            raise RuntimeError("simulated crash after commit")
        return result

    def _check_recipient(self, c, to):
        if to == c["seat"]:
            raise Refuse("E_SEAT", "that is your own seat", what="recipient")
        r = self.roster.get(to, {})
        if r.get("where"):
            raise Refuse("E_SEAT", f"{to} is on {r['where']}: mail cannot reach it from this machine", what="recipient")
        if r.get("retired"):
            raise Refuse("E_SEAT", f"{to} is retired", what="recipient")
        if to not in self.roster and not self.db.execute("SELECT 1 FROM seat WHERE name=?", (to,)).fetchone():
            raise Refuse("E_SEAT", f"no seat {to}: nothing has leased it and the roster does not list it",
                         what="recipient")

    def _delivery(self, to, now) -> str:
        if self.roster.get(to, {}).get("reads") == "on request":
            return f"kept: {to} reads on request"
        if self.running(to):
            return f"delivered: {to} is running"
        row = self.db.execute("SELECT last_seen_ts FROM seat WHERE name=?", (to,)).fetchone()
        if row and row[0]:
            return f"kept: {to} is not running, last seen {age(now - row[0])[:-1]} {age(now - row[0])[-1]} ago"
        return f"kept: {to} is not running, never leased"

    def _limits(self, c, to, kind, nbytes, now):
        q = lambda sql, *a: self.db.execute(sql, a).fetchone()[0] or 0   # noqa: E731
        seat, acct = c["seat"], c["account"]
        for n, cap, what in (
                (q("SELECT count(*) FROM msg WHERE from_seat=? AND ts>?", seat, now - HOUR),
                 SEAT_PER_HOUR, "sends an hour from one seat"),
                (q("SELECT count(*) FROM msg WHERE from_seat=? AND to_seat=? AND ts>?", seat, to,
                   now - HOUR), PAIR_PER_HOUR, "sends an hour to one seat (pair)"),
                (q("SELECT count(*) FROM msg WHERE from_account=? AND ts>?", acct, now - HOUR),
                 ACCOUNT_PER_HOUR, "sends an hour from one account")):
            if n >= cap:
                raise Refuse("E_LIMIT", f"rate limit: {cap} {what}", rate=True)
        for since, cap, what in ((now - HOUR, BYTES_PER_HOUR, "1 MiB of bodies an hour"),
                                 (now - DAY, BYTES_PER_DAY, "4 MiB of bodies a day")):
            if q("SELECT sum(bytes) FROM msg WHERE from_seat=? AND ts>?", seat, since) + nbytes > cap:
                raise Refuse("E_LIMIT", f"rate limit: {what} from one seat", rate=True)
        if kind == "request":
            cond, cap, per, what = "kind='request'", OPEN_REQ_CAP, OPEN_REQ_PER_SENDER, "open requests"
        else:
            cond, cap, per, what = "kind!='request'", UNACKED_CAP, UNACKED_PER_SENDER, "unacked items"
        base = f"SELECT count(*) FROM msg WHERE to_seat=? AND {cond} AND state='open' AND acked_ts IS NULL"
        if q(base, to) >= cap:
            raise Refuse("E_FULL", f"{to} has {cap} {what} waiting")
        if q(base + " AND from_seat=?", to, seat) >= per:
            raise Refuse("E_FULL", f"{to} has {per} {what} waiting from {seat}")

    # reading
    def _view(self, m, now, body=False) -> dict:
        v = {"id": m["id"], "ts": m["ts"], "from": m["from_seat"], "from_account": m["from_account"],
             "level": m["level"], "from_kind": m["from_kind"], "from_pid": m["from_pid"], "to": m["to_seat"],
             "kind": m["kind"], "re": m["re"], "subject": m["subject"], "bytes": m["bytes"], "sha256": m["sha256"],
             "state": m["state"], "fact": self._fact(m), "age": age(now - m["ts"]),
             "acked": m["acked_ts"] is not None, "note": m["note"], "answer": m["answer_id"]}
        if body:
            v["body"], v["refs"] = m["body"], json.loads(m["refs"] or "[]")
        return v

    @staticmethod
    def _fact(m) -> str:
        if m["acked_ts"] is not None:
            return "acked"
        return "seen" if m["seen_ts"] else "surfaced" if m["surfaced_ts"] else "never surfaced"

    def op_inbox(self, c, req, body, now, waiter):
        if req.get("all"):
            where, args = "to_seat=? AND ts>?", (c["seat"], now - ITEM_LIFE)
        else:
            where, args = "to_seat=? AND state='open' AND acked_ts IS NULL", (c["seat"],)
        rows = self.db.execute(f"SELECT * FROM msg WHERE {where} ORDER BY ts", args).fetchall()
        return {"ok": True, "seat": c["seat"], "items": [self._view(m, now) for m in rows]}

    def _party(self, c, m, roles=("to", "from")):
        if "to" in roles and m["to_seat"] == c["seat"] and m["to_account"] == c["account"]:
            return "to"
        if "from" in roles and m["from_seat"] == c["seat"] and m["from_account"] == c["account"]:
            return "from"
        raise Refuse("E_PARTY", "only the recipient or the sender may do that")

    def op_read(self, c, req, body, now, waiter):
        m = self._get(req.get("msg"))
        role = self._party(c, m)
        if role == "to":
            if m["seen_ts"] is None:
                with self.tx():
                    self.db.execute("UPDATE msg SET seen_ts=? WHERE id=?", (now, m["id"]))
                m = self._get(m["id"])
            self._log("recv", name=c["seat"], ids=[m["id"]], kind=c["kind"], pid=c["pid"], start=c["start"],
                      account=c["account"])
        return {"ok": True, "msg": self._view(m, now, body=True), "role": role}

    def op_refs(self, c, req, body, now, waiter):
        m = self._get(req.get("msg"))
        if not (m["to_seat"] == c["seat"] and m["to_account"] == c["account"]):
            raise Refuse("E_PARTY", "only the recipient may check references")
        return {"ok": True, "refs": json.loads(m["refs"] or "[]")}

    def op_ack(self, c, req, body, now, waiter):
        ids = req.get("msgs")
        if not isinstance(ids, list) or not 1 <= len(ids) <= 100:
            raise Refuse("E_FORMAT", "ack takes 1 to 100 ids")
        done, key = self._retry(c, req, body)
        if done:
            return done
        rows = [self._get(i) for i in ids]
        for m in rows:
            self._party(c, m, roles=("to",))
        new = [m["id"] for m in rows if m["acked_ts"] is None]
        result = {"ok": True, "acked": new, "already": [m["id"] for m in rows if m["acked_ts"] is not None]}
        with self.tx():
            for i in new:
                self.db.execute("UPDATE msg SET acked_ts=?, state=CASE WHEN kind!='request' AND state="
                                "'open' THEN 'acked' ELSE state END, state_ts=CASE WHEN kind!='request' "
                                "AND state='open' THEN ? ELSE state_ts END WHERE id=?", (now, now, i))
            self._keep(c, key, result, ids[0], now)
        if new:
            self._log("ack", name=c["seat"], ids=new, kind=c["kind"], pid=c["pid"], account=c["account"])
        return result

    def op_out(self, c, req, body, now, waiter):
        rows = self.db.execute("SELECT * FROM msg WHERE from_seat=? AND ts>? ORDER BY ts",
                               (c["seat"], now - ITEM_LIFE)).fetchall()
        items = []
        for m in rows:
            v = self._view(m, now)
            if m["stuck_ts"] and m["acked_ts"] is None and m["state"] == "open":
                v["stuck"] = self._stuck_reason(m)
            items.append(v)
        return {"ok": True, "items": items}

    # request states (§3.5)
    def op_close(self, c, req, body, now, waiter):
        note = req.get("note")
        if note is not None and (not isinstance(note, str) or "\n" in note or len(note) > MAX_NOTE):
            raise Refuse("E_SIZE", f"note: one line of at most {MAX_NOTE} characters")
        done, key = self._retry(c, req, body)
        if done:
            return done
        m = self._get(req.get("msg"))
        if m["kind"] != "request":
            raise Refuse("E_KIND", f"{m['id']} is a {m['kind']}; only requests are closed")
        declined = bool(req.get("declined"))
        if self._party(c, m) != ("to" if declined else "from"):
            raise Refuse("E_PARTY", "the recipient declines (--declined); the sender withdraws (no flag)")
        if m["state"] != "open":
            raise Refuse("E_STATE", f"request {m['id']} is {m['state']}")
        state = "declined" if declined else "withdrawn"
        result = {"ok": True, "id": m["id"], "state": state}
        with self.tx():
            self.db.execute("UPDATE msg SET state=?, state_ts=?, note=?, notified_ts=? WHERE id=?",
                            (state, now, note, None if declined else now, m["id"]))
            self._keep(c, key, result, m["id"], now)
        self._log("state", id=m["id"], state=state, name=c["seat"])
        self._wake(m["id"])
        return result

    # waiting for one reply (§4.3)
    def op_wait(self, c, req, body, now, waiter):
        m = self._get(req.get("msg"))
        if m["kind"] != "request":
            raise Refuse("E_KIND", f"{m['id']} is a {m['kind']}; wait --re takes a request")
        self._party(c, m, roles=("from",))
        if m["state"] != "open":
            self._surface_answer(m, c["seat"], c["kind"], c["pid"], c["account"])
            return {"ok": True, "done": self.wait_line(m)}
        if waiter is None:
            raise Refuse("E_FORMAT", "wait needs a connection")
        held = list(self.waits.values())
        if sum(1 for w in held if w[0] == m["id"]) >= WAIT_REQUEST:
            raise Refuse("E_LIMIT", "a wait for this request is already running")
        if sum(1 for w in held if w[1] == c["seat"]) >= WAIT_SEAT:
            raise Refuse("E_LIMIT", f"{WAIT_SEAT} waits per seat")
        if sum(1 for w in held if w[2] == c["account"]) >= WAIT_ACCOUNT:
            raise Refuse("E_LIMIT", f"{WAIT_ACCOUNT} waits per account")
        self.waits[waiter] = (m["id"], c["seat"], c["account"], c["kind"], c["pid"])
        return {"ok": True, "waiting": True}

    def drop_wait(self, waiter):
        self.waits.pop(waiter, None)

    def wait_line(self, m) -> str:
        s = f"request {m['id']} to {m['to_seat']}: {m['state']}"
        if m["state"] == "answered":
            a = self.db.execute("SELECT * FROM msg WHERE id=?", (m["answer_id"],)).fetchone()
            if a:
                s += (f" by report {a['id']} from {a['from_seat']} [{a['level']}], subject in the sender's words: "
                      f"{quote(a['subject'])}. Read it with: agentmail read {a['id']}")
        elif m["state"] == "declined" and m["note"]:
            s += f", note in the recipient's words: {quote(m['note'])}"
        return s

    def _surface_answer(self, m, seat, kind, pid, account):
        """wait --re printed the answering report's header: that is surfacing it, as a hook would."""
        if m["state"] != "answered" or not m["answer_id"]:
            return
        a = self.db.execute("SELECT * FROM msg WHERE id=?", (m["answer_id"],)).fetchone()
        if a and a["to_seat"] == seat and a["surfaced_ts"] is None:
            with self.tx():
                self.db.execute("UPDATE msg SET surfaced_ts=? WHERE id=?", (self.clock(), a["id"]))
            self._log("surfaced", name=seat, ids=[a["id"]], kind=kind, pid=pid, account=account)

    def _wake(self, msg_id):
        m = self.db.execute("SELECT * FROM msg WHERE id=?", (msg_id,)).fetchone()
        for waiter, w in list(self.waits.items()):
            if w[0] == msg_id:
                del self.waits[waiter]
                self._surface_answer(m, w[1], w[3], w[4], w[2])
                self.ready.append((waiter, {"ok": True, "done": self.wait_line(m)}))

    # seats and the desk
    def op_seats(self, c, req, body, now, waiter):
        names = {r["name"]: r for r in self.db.execute("SELECT * FROM seat").fetchall()}
        out = []
        for name in sorted(set(names) | set(self.roster)):
            lease, row, cfg = self._lease(name), names.get(name), self.roster.get(name, {})
            if lease and not self.is_alive(self._lident(lease)):
                self._end_lease(name, lease, now)
                lease, row = None, self.db.execute("SELECT * FROM seat WHERE name=?", (name,)).fetchone()
            if lease and self.db.execute("SELECT 1 FROM conflict WHERE seat=? AND holder_pid=? AND ts>?",
                                         (name, lease["pid"], now - DAY)).fetchone():
                status = f"conflict (held by {lease['kind']} pid {lease['pid']})"
            elif lease:
                status = f"running ({lease['kind']} pid {lease['pid']})"
            elif row and row["last_seen_ts"]:
                status = f"not running (last seen {age(now - row['last_seen_ts'])} ago)"
            else:
                status = "never leased"
            notes = [k for k in ("overview", "retired") if cfg.get(k)]
            notes += ([f"reads {cfg['reads']}"] if cfg.get("reads") else []) + (
                [f"on {cfg['where']}"] if cfg.get("where") else []) + (
                [f"also {', '.join(cfg['names'])}"] if cfg.get("names") else [])
            out.append({"name": name, "status": status, "notes": notes})
        return {"ok": True, "you": c["seat"], "seats": out}

    def _stuck_reason(self, m) -> str:
        if not self.running(m["to_seat"]):
            return "seat not running"
        if m["surfaced_ts"] is None:
            return "never surfaced"
        return "surfaced, not read" if m["seen_ts"] is None else "read, not acknowledged"

    def desk_lines(self, now) -> list[str]:
        lines = []
        for m in self.db.execute("SELECT * FROM msg WHERE state='open' AND (kind='request' OR "
                                 "(stuck_ts IS NOT NULL AND acked_ts IS NULL)) ORDER BY ts").fetchall():
            stuck = m["stuck_ts"] is not None and m["acked_ts"] is None
            fact = self._stuck_reason(m) if stuck else self._fact(m)
            lines.append(f"{'stuck' if stuck else 'open':<8} {m['to_seat']:<14} {m['kind']:<7} {m['id']} "
                         f"from {m['from_seat']:<13} {age(now - m['ts']):>3}   {fact}")
        for r in self.db.execute("SELECT * FROM conflict WHERE ts>? ORDER BY ts", (now - DAY,)).fetchall():
            lines.append(f"{'conflict':<8} {r['seat']:<14} pid {r['holder_pid']} holds; pid {r['claimant_pid']} "
                         f"refused {hhmm(r['ts'])}")
        for r in self.db.execute("SELECT * FROM seat WHERE first_ts>? AND first_pid IS NOT NULL ORDER BY first_ts",
                                 (now - DAY,)).fetchall():
            lines.append(f"{'new seat':<8} {r['name']:<14} first leased {hhmm(r['first_ts'])} by pid {r['first_pid']}")
        if self.clock_fault:
            lines.append(f"{'clock':<8} {self.clock_state()}")
        return lines

    def desk_summary(self) -> str:
        rows = self.db.execute("SELECT to_seat, count(*) FROM msg WHERE kind='request' AND state='open' "
                               "GROUP BY to_seat ORDER BY count(*) DESC, to_seat").fetchall()
        n = sum(r[1] for r in rows)
        stuck = self.db.execute("SELECT count(*) FROM msg WHERE state='open' AND stuck_ts IS NOT NULL "
                                "AND acked_ts IS NULL").fetchone()[0]
        per = f" ({', '.join(f'{r[0]} {r[1]}' for r in rows)})" if rows else ""
        return f"desk: {n} open{per}, {stuck} stuck" + (", clock fault" if self.clock_fault else "")

    def op_desk(self, c, req, body, now, waiter):
        if not self.roster.get(c["seat"], {}).get("overview"):
            raise Refuse("E_OWNER", "the desk is for seats with overview = true in the roster")
        return {"ok": True, "lines": self.desk_lines(now)}

    def op_register(self, c, req, body, now, waiter):
        if c["seat"]:
            self._cmd_alias("register", c, now)
            return {"ok": True, "seat": c["seat"], "automatic": True}
        if not c["nameless"]:
            raise Refuse("E_SEAT", "only a Claude Code or Codex process can hold a seat", what="no_seat")
        name = _fit(req.get("name") if isinstance(req.get("name"), str) else None)
        if not name:
            raise Refuse("E_SEAT", "a seat name matches [a-z][a-z0-9-]{1,31}")
        name = self.alias_of(name)
        if self.roster.get(name, {}).get("spool"):
            raise Refuse("E_SEAT", f"seat {name} is reached only through its own spool, by the Codex app-server",
                         what="spool_seat", seat=name)
        lease = self._lease(name)
        if lease and self.is_alive(self._lident(lease)):
            raise Refuse("E_HELD", f"seat {name} is held by pid {lease['pid']}", seat=name, holder=lease["pid"])
        if lease:
            self._end_lease(name, lease, now)
        self._new_lease(name, c["ident"], c["account"], now)
        return {"ok": True, "seat": name}

    # surfacing (§4.1)
    def op_hook(self, c, req, body, now, waiter):
        event = req.get("event")
        if event not in ("session-start", "prompt"):
            raise Refuse("E_FORMAT", "hook event is session-start or prompt")
        seat, start = c["seat"], event == "session-start"
        rows = self.db.execute("SELECT * FROM msg WHERE to_seat=? AND state='open' AND acked_ts IS NULL "
                               + ("" if start else "AND surfaced_ts IS NULL ") + "ORDER BY ts", (seat,)).fetchall()
        changes = self.db.execute("SELECT * FROM msg WHERE from_seat=? AND kind='request' AND notified_ts IS NULL "
                                  "AND state IN ('declined','expired') ORDER BY state_ts", (seat,)).fetchall()
        stuck = self.db.execute("SELECT * FROM msg WHERE from_seat=? AND stuck_ts IS NOT NULL AND "
                                "stuck_notified_ts IS NULL AND acked_ts IS NULL AND state='open' ORDER BY ts",
                                (seat,)).fetchall()
        lines, marks, notes, stucks = [], [], [], []
        for m in rows:
            if len(lines) < 10:
                lines.append("  " + header_line(self._view(m, now)))
                if m["surfaced_ts"] is None:
                    marks.append(m["id"])
        for m in changes:
            if len(lines) < 10:
                s = f"  request {m['id']} to {m['to_seat']}: {m['state']}"
                if m["state"] == "declined" and m["note"]:
                    s += f", note in the recipient's words: {quote(m['note'])}"
                lines.append(s)
                notes.append(m["id"])
        for m in stuck:
            if len(lines) < 10:
                lines.append(f"  stuck: {m['kind']} {m['id']} to {m['to_seat']}, {age(now - m['ts'])}, "
                             f"{self._stuck_reason(m)}")
                stucks.append(m["id"])
        more = len(rows) + len(changes) + len(stuck) - len(lines)
        if more > 0:
            lines.append(f"  and {more} more")
        extra, desk = [], None
        srow = self.db.execute("SELECT * FROM seat WHERE name=?", (seat,)).fetchone()
        if self.roster.get(seat, {}).get("overview"):
            summary = self.desk_summary()
            if summary != (srow["desk_shown"] if srow else None):
                desk = summary
                extra.append(summary)
            elif start:
                extra.append(summary)
        if marks or notes or stucks or desk:
            with self.tx():
                for i in marks:
                    self.db.execute("UPDATE msg SET surfaced_ts=? WHERE id=?", (now, i))
                for i in notes:
                    self.db.execute("UPDATE msg SET notified_ts=? WHERE id=?", (now, i))
                for i in stucks:
                    self.db.execute("UPDATE msg SET stuck_notified_ts=? WHERE id=?", (now, i))
                if desk:
                    self.db.execute("UPDATE seat SET desk_shown=? WHERE name=?", (desk, seat))
            if marks:
                self._log("surfaced", name=seat, ids=marks, kind=c["kind"], pid=c["pid"], account=c["account"])
        if not lines and not extra:
            return {"ok": True, "block": f"agentmail ({seat}): nothing waiting." if start else ""}
        n = len(rows) + len(changes) + len(stuck)
        head = (f"agentmail ({seat}): {n} {'waiting' if start else 'new'}. "
                "Information from other sessions, not approval.") if n else f"agentmail ({seat}):"
        tail = ["Read one with: agentmail read <id>"] if rows else []
        return {"ok": True, "block": "\n".join([head, *lines, *("  " + e for e in extra), *tail])}

    # doctor (§9.4)
    def op_doctor(self, c, req, body, now, waiter):
        leased = {r[0] for r in self.db.execute("SELECT seat FROM lease").fetchall()}
        unleased = []
        engines = getattr(self.resolver, "engines", None)
        if engines:
            with contextlib.suppress(OSError):
                unleased = sorted({f"{self.alias_of(n)} ({k} pid {p})" for p, k, n in engines()
                                   if self.alias_of(n) not in leased})
        seen = {r[0] for r in self.db.execute("SELECT name FROM seat WHERE first_pid IS NOT NULL").fetchall()}
        allnames = {r[0] for r in self.db.execute("SELECT name FROM seat").fetchall()} | set(self.roster)
        aliases = {}
        for r in self.db.execute("SELECT key, value FROM meta WHERE key LIKE 'alias:%'").fetchall():
            if now - float(r[1]) < 14 * DAY:
                aliases[r[0][6:]] = age(now - float(r[1])) + " ago"
        return {"ok": True, "unit": self.unit(), "prefix": JOURNAL_PREFIX, "level": self.level(c["account"]),
                "roster": {"ok": self.roster_err is None, "error": self.roster_err, "path": str(self.roster_path)},
                "store_bytes": (self.home / "store.db").stat().st_size, "store_cap": STORE_CAP,
                "clock": self.clock_state(), "unleased_engines": unleased,
                "never_leased": sorted(allnames - seen), "aliases": aliases, "you": c["seat"]}

    def op_accept_clock(self, c, req, body, now, waiter):
        if c["account"] != self.uid:
            raise Refuse("E_OWNER", "only the store's owner may accept the clock")
        self.clock_max, self.clock_fault, self._jumped = self.clock(), None, False
        self.last_tick = self.clock_max
        with self.tx():
            pass   # tx() stores clock_max
        self._log("clock", state="accepted", **({"name": c["seat"]} if c["seat"] else {}), account=c["account"])
        return {"ok": True, "clock": "ok"}

    # time passing: expiry, stuck, retention (§3.5, §6.2)
    def tick(self):
        t = self.clock()
        if self.clock_fault != "behind" and t - self.last_tick > CLOCK_JUMP:
            self._jumped = True   # sticky until doctor --accept-clock
        self.last_tick = max(self.last_tick, t)
        now = self._now()
        self._refused_flush(now)
        self.roster_check()
        for row in self.db.execute("SELECT * FROM lease").fetchall():
            if not self.is_alive(self._lident(row)):
                self._end_lease(row["seat"], row, now)
        if self.clock_fault:
            return   # §6.2: expire nothing, delete nothing
        q = lambda sql, *a: self.db.execute(sql, a).fetchall()   # noqa: E731
        expire = q("SELECT * FROM msg WHERE state='open' AND acked_ts IS NULL AND ttl_ts<=?", now) + \
            q("SELECT * FROM msg WHERE kind='request' AND state='open' AND acked_ts IS NOT NULL AND ttl_ts<=?", now)
        stuck = q("SELECT * FROM msg WHERE state='open' AND acked_ts IS NULL AND stuck_ts IS NULL AND ts<=?",
                  now - STUCK_AFTER)
        bodies = q("SELECT id FROM msg WHERE body IS NOT NULL AND ((acked_ts IS NOT NULL AND acked_ts<=?) OR "
                   "(state IN ('answered','declined','withdrawn') AND state_ts<=?))", now - BODY_KEEP, now - BODY_KEEP)
        old = q("SELECT id FROM msg WHERE ts<=? AND state!='open'", now - ROW_KEEP)
        if expire or stuck or bodies or old or (self.clock_max or 0) - (self._clock_saved or 0) > HOUR:
            with self.tx():
                for m in expire:
                    # an expired request stays open to nobody; an unacked item simply goes
                    self.db.execute("UPDATE msg SET state='expired', state_ts=?, body=NULL WHERE id=?", (now, m["id"]))
                for m in stuck:
                    self.db.execute("UPDATE msg SET stuck_ts=? WHERE id=?", (now, m["id"]))
                for m in bodies:
                    self.db.execute("UPDATE msg SET body=NULL WHERE id=?", (m["id"],))
                for m in old:
                    self.db.execute("DELETE FROM msg WHERE id=?", (m["id"],))
                    self.db.execute("DELETE FROM req WHERE msg_id=?", (m["id"],))
                self.db.execute("DELETE FROM req WHERE ts<=? AND msg_id IS NULL", (now - ROW_KEEP,))
                self.db.execute("DELETE FROM conflict WHERE ts<=?", (now - 7 * DAY,))
        for m in expire:
            self._log("state", id=m["id"], state="expired")
            if m["kind"] == "request":
                self._wake(m["id"])
        for m in stuck:
            self._log("stuck", id=m["id"], to=m["to_seat"], **{"from": m["from_seat"]}, reason=self._stuck_reason(m))


def header_line(v: dict) -> str:
    """One item, as hooks and inbox show it. The subject is always labelled as the sender's words."""
    s = f"{v['kind']:<7} {v['id']} from {v['from']} [{v['level']}]"
    if v.get("re"):
        s += f" answers {v['re']}"
    return s + f", subject in the sender's words: {quote(v['subject'])}"


def own_unit() -> str | None:
    """The systemd unit this process runs in, from its cgroup (doctor's unit check)."""
    try:
        for line in Path("/proc/self/cgroup").read_text().splitlines():
            last = line.rsplit("/", 1)[-1]
            if last.endswith(".service"):
                return last
    except OSError:
        pass
    return None


# ---- the daemon: one loop (§6.1) -----------------------------------------------------------

class Conn:
    def __init__(self, sock, uid, pid, ident, key):
        self.sock, self.uid, self.pid, self.ident, self.key = sock, uid, pid, ident, key
        self.t0, self.buf, self.req, self.line_len = time.monotonic(), b"", None, 0
        self.out, self.closing, self.waiting = b"", False, False


class SpoolWaiter:
    def __init__(self, rid, pid, start, where=(None, None)):
        self.rid, self.pid, self.start, self.where = rid, pid, start, where


class SpoolUnsafe(Exception):
    """A seat spool directory the daemon will not serve."""


def open_seat_dir(path: str, sub: str, uid: int) -> int:
    """A descriptor for <path>/<sub> of a seat spool. Every component is opened without following a link,
    so no symlink anywhere in the path; the spool and `sub` must be directories owned by `uid` that no group
    or other can write. `sub` (req or rep) is created, mode 700, when missing; the spool itself never is."""
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        parts = Path(path).parts[1:]
        for i, part in enumerate([*parts, sub]):
            if i == len(parts):
                with contextlib.suppress(FileExistsError):
                    os.mkdir(part, 0o700, dir_fd=fd)
            try:
                nfd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            except OSError as e:
                if e.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise SpoolUnsafe(f"{part} is a symbolic link or not a directory") from None
                raise
            os.close(fd)
            fd = nfd
            if i >= len(parts) - 1:
                st = os.fstat(fd)
                if st.st_uid != uid:
                    raise SpoolUnsafe(f"{part} is owned by uid {st.st_uid}, not the daemon's {uid}")
                if st.st_mode & 0o022:
                    raise SpoolUnsafe(f"{part} is writable by group or others (mode {st.st_mode & 0o777:o})")
        return fd
    except BaseException:
        os.close(fd)
        raise


def holders(st: os.stat_result, proc: str = "/proc") -> list[int]:
    """Processes holding the inode open (the shared spool's identity)."""
    out = []
    for entry in os.scandir(proc):
        if not entry.name.isdigit():
            continue
        try:
            fds = os.scandir(f"{proc}/{entry.name}/fd")
        except OSError:
            continue
        with fds:
            for fd in fds:
                try:
                    s = os.stat(fd.path)
                except OSError:
                    continue
                if s.st_ino == st.st_ino and s.st_dev == st.st_dev:
                    out.append(int(entry.name))
                    break
    return out


def parse_frame(raw: bytes) -> tuple[object, bytes | None]:
    """A request: one JSON line (4 KiB at most), then for `send` a 4-byte length and the body."""
    if b"\n" not in raw:
        raise Refuse("E_FORMAT", "a request is one JSON line")
    line, rest = raw.split(b"\n", 1)
    if len(line) > MAX_LINE:
        raise Refuse("E_FORMAT", f"the request line is over {MAX_LINE} bytes")
    try:
        req = json.loads(line)
    except ValueError:
        raise Refuse("E_FORMAT", "the request line is not JSON") from None
    if not (isinstance(req, dict) and req.get("op") == "send"):
        return req, None
    if len(rest) < 4:
        raise Refuse("E_FORMAT", "send needs its body frame")
    n = struct.unpack(">I", rest[:4])[0]
    if len(rest) - 4 != n:
        raise Refuse("E_FORMAT", "the body frame has the wrong length")
    return req, rest[4:]


class Server:
    def __init__(self, box: Mailbox, home: Path, stop: threading.Event | None = None,
                 spool_interval: float = 1.0, tick_interval: float = 60.0, first_line_s: float = 1.0,
                 request_s: float = 10.0, uid: int | None = None):
        """`uid`: tests only, the owner a seat spool must have (default: this process's uid)."""
        self.box, self.home, self.stop = box, Path(home), stop
        self.spool_interval, self.tick_interval = spool_interval, tick_interval
        self.first_line_s, self.request_s = first_line_s, request_s   # §6.1: 1 s, then 10 s in all
        self.sel = selectors.DefaultSelector()
        self.conns: set[Conn] = set()
        self.spool_waits: set[SpoolWaiter] = set()
        self.pidfds: dict[int, tuple] = {}
        self.uid = os.geteuid() if uid is None else uid
        self._spool_gen, self._spool_bad = None, set()   # roster load seen; seat spools journaled as unsafe

    def watch(self, seat, ident) -> bool:
        """pidfd on the engine process, then its start time read again: same process, or none."""
        try:
            fd = os.pidfd_open(ident["pid"])
        except OSError:
            return False
        st = read_stat(ident["pid"])
        if st is None or st[1] != ident["start"]:
            os.close(fd)
            return False
        self.pidfds[fd] = (seat, ident)
        self.sel.register(fd, selectors.EVENT_READ, ("pidfd", fd))
        return True

    def run(self, ready: threading.Event | None = None):
        home = self.home
        for d in ("spool", "spool/req", "spool/rep"):
            (home / d).mkdir(mode=0o700, exist_ok=True)
        path = home / "sock"
        path.unlink(missing_ok=True)   # we hold the one-writer lock, so any socket here is stale
        lsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        old = os.umask(0o177)
        try:
            lsock.bind(str(path))
        finally:
            os.umask(old)
        lsock.listen(128)
        lsock.setblocking(False)
        self.sel.register(lsock, selectors.EVENT_READ, ("listen", None))
        if self.box.watch is None:
            self.box.watch = self.watch
        self.box.rewatch()
        self._spools()   # seat spools' req and rep exist (or are journaled as unsafe) before anyone is told ready
        if ready:
            ready.set()
        next_spool = next_tick = 0.0
        try:
            while not (self.stop and self.stop.is_set()):
                for key, mask in self.sel.select(0.1 if self.stop else 0.5):
                    kind, obj = key.data
                    if kind == "listen":
                        self._accept(lsock)
                    elif kind == "pidfd":
                        self.sel.unregister(obj)
                        seat, ident = self.pidfds.pop(obj)
                        os.close(obj)
                        self.box.lease_gone(seat, ident)
                    elif mask & selectors.EVENT_READ:
                        self._read(obj)
                    if kind == "conn" and mask & selectors.EVENT_WRITE:
                        self._write(obj)
                t = time.monotonic()
                for c in list(self.conns):
                    limit = self.first_line_s if c.req is None and b"\n" not in c.buf else self.request_s
                    if not c.waiting and not c.closing and t - c.t0 > limit:
                        self._refuse(c, Refuse("E_FORMAT", "no request line within 1 second" if c.req is None
                                               else "request too slow"))
                    elif c.closing and t - c.t0 > 30:
                        self._close(c)
                if t >= next_spool:
                    next_spool = t + self.spool_interval
                    self._spool()
                if t >= next_tick:
                    next_tick = t + self.tick_interval
                    self.box.tick()
                self._deliver()
        finally:
            with contextlib.suppress(Exception):
                self.box.flush_refused()
            for c in list(self.conns):
                self._close(c)
            for fd in list(self.pidfds):
                os.close(fd)
            self.sel.close()
            lsock.close()
            path.unlink(missing_ok=True)

    def _accept(self, lsock):
        while True:
            try:
                sock, _ = lsock.accept()
            except (BlockingIOError, InterruptedError):
                return
            pid, uid, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid not in self.box.accounts:
                sock.close()   # §6.1: the uid first; nothing the client says about itself
                self.box.log_refused(uid, code="E_OWNER", what="uid", req_op="?", pid=pid)
                continue
            sock.setblocking(False)
            try:
                ident = self.box.resolver(pid)
                key = self.box.seat_key(ident)
            except Exception:   # noqa: BLE001 - a broken walk is a caller with no seat
                ident, key = {"kind": None, "pid": pid, "start": 0, "name": None}, None
            c = Conn(sock, uid, pid, ident, key)
            live = [x for x in self.conns if not x.closing or x.waiting]
            same = lambda f: sum(1 for x in live if f(x))   # noqa: E731
            if (len(live) >= CONN_TOTAL or same(lambda x: x.uid == uid) >= CONN_ACCOUNT
                    or (key and same(lambda x: x.key == key) >= CONN_SEAT)
                    or (not key and same(lambda x: x.key is None) >= CONN_NOSEAT)):
                c.out, c.closing = json.dumps({"ok": False, "code": "E_LIMIT", "error": "too many connections",
                                               "retry": True}).encode() + b"\n", True
                self.box.log_refused(uid, code="E_LIMIT", what="conn_cap", req_op="connect", pid=pid)
            self.conns.add(c)
            self.sel.register(sock, selectors.EVENT_READ | (selectors.EVENT_WRITE if c.out else 0), ("conn", c))

    def _read(self, c: Conn):
        try:
            data = c.sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            data = b""
        if not data:
            if c.waiting:
                self.box.drop_wait(c)   # §4.3: ending the process frees its slot at once
            self._close(c)
            return
        if c.waiting or c.closing:
            return   # nothing more is read from a connection that has its request
        c.buf += data
        if len(c.buf) > MAX_REQUEST:
            return self._refuse(c, Refuse("E_SIZE", f"a request is at most {MAX_REQUEST} bytes"))
        if b"\n" not in c.buf:
            if len(c.buf) > MAX_LINE:
                self._refuse(c, Refuse("E_FORMAT", f"the request line is over {MAX_LINE} bytes"))
            return
        line = c.buf.split(b"\n", 1)[0]
        try:
            head = json.loads(line) if len(line) <= MAX_LINE else None
        except ValueError:
            head = None
        if isinstance(head, dict) and head.get("op") == "send":
            rest = c.buf[len(line) + 1:]
            if len(rest) >= 4 and len(line) + 5 + struct.unpack(">I", rest[:4])[0] > MAX_REQUEST:
                return self._refuse(c, Refuse("E_SIZE", f"a request is at most {MAX_REQUEST} bytes"))
            if len(rest) < 4 or len(rest) - 4 < struct.unpack(">I", rest[:4])[0]:
                c.req = head   # line arrived; the frame has 10 seconds from the start
                return
        try:
            req, body = parse_frame(c.buf)
        except Refuse as e:
            return self._refuse(c, e)
        c.req = req
        try:
            reply = self.box.handle(c.pid, c.uid, req, body, ident=c.ident, waiter=c)
        except Exception as e:   # noqa: BLE001 - one bad request must not stop the mailbox
            print(f"agentmail: error handling {req.get('op') if isinstance(req, dict) else '?'!r}: {e!r}",
                  file=sys.stderr, flush=True)
            self._close(c)
            return
        if reply.get("waiting"):
            c.waiting = True
        else:
            self._reply(c, reply)

    def _refuse(self, c: Conn, e: Refuse):
        """A refusal sent before handle(): a frame, size or time fault on the socket."""
        self.box.log_refused(c.uid, code=e.code, what="frame", req_op="?", pid=c.pid)
        self._reply(c, e.reply())

    def _reply(self, c: Conn, reply: dict):
        c.out += json.dumps(reply).encode() + b"\n"
        c.closing = True
        self._write(c)

    def _write(self, c: Conn):
        try:
            n = c.sock.send(c.out)
            c.out = c.out[n:]
        except (BlockingIOError, InterruptedError):
            pass
        except OSError:
            c.out = b""
        if c.closing and not c.out:
            return self._close(c)
        with contextlib.suppress(KeyError, ValueError):
            self.sel.modify(c.sock, selectors.EVENT_READ | (selectors.EVENT_WRITE if c.out else 0), ("conn", c))

    def _close(self, c: Conn):
        if c in self.conns:
            self.conns.discard(c)
            if c.waiting:
                self.box.drop_wait(c)
            with contextlib.suppress(KeyError, ValueError):
                self.sel.unregister(c.sock)
            c.sock.close()

    def _deliver(self):
        ready, self.box.ready = self.box.ready, []
        for waiter, reply in ready:
            if isinstance(waiter, Conn) and waiter in self.conns:
                waiter.waiting = False
                self._reply(waiter, reply)
            elif isinstance(waiter, SpoolWaiter):
                self.spool_waits.discard(waiter)
                self._spool_reply(waiter.rid, reply, waiter.where)

    # spool transport (Codex): unchanged identity rule, once a second (§6.1). Besides the shared spool,
    # every roster seat with `spool` is served the same way; there the folder decides the seat.
    def _spools(self) -> list[tuple]:
        """(seat, path) of every spool to serve this pass; (None, None) is the shared one. An unsafe seat
        spool is skipped, with one `refused` line per roster load."""
        box = self.box
        if self._spool_gen != box.roster_gen:
            self._spool_gen, self._spool_bad = box.roster_gen, set()
        out = [(None, None)]
        for seat, cfg in sorted(box.roster.items()):
            if not cfg.get("spool"):
                continue
            where = (seat, cfg["spool"])
            try:
                for sub in ("req", "rep"):
                    os.close(self._open(where, sub))
            except (OSError, SpoolUnsafe) as e:
                if seat not in self._spool_bad:
                    self._spool_bad.add(seat)
                    why = e.strerror if isinstance(e, OSError) and e.strerror else str(e)
                    box.log_refused(box.uid, key_extra=(seat, box.roster_gen), code="E_FORMAT", what="roster",
                                    req_op="?", name=seat,
                                    error=clean(f"spool {cfg['spool']} not served: {why}", False)[:200])
                continue
            out.append(where)
        return out

    def _open(self, where, sub) -> int:
        seat, path = where
        if seat is None:
            return os.open(self.home / "spool" / sub, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        if _inside(os.path.realpath(path), os.path.realpath(self.home / "spool")):
            raise SpoolUnsafe("it is the shared spool or lies inside it")
        return open_seat_dir(path, sub, self.uid)

    def _spool(self):
        served = self._spools()
        for where in served:
            try:
                dfd = self._open(where, "req")
            except (OSError, SpoolUnsafe):
                continue
            try:
                for name in sorted(os.listdir(dfd)):
                    if re.fullmatch(r"[0-9a-f]{32}\.json", name):
                        self._spool_one(dfd, name, where)
            finally:
                os.close(dfd)
        for t in list(self.spool_waits):
            st = read_stat(t.pid)
            if st is None or st[1] != t.start:
                self.spool_waits.discard(t)
                self.box.drop_wait(t)
        for where in served:   # replies nobody collected
            try:
                dfd = self._open(where, "rep")
            except (OSError, SpoolUnsafe):
                continue
            try:
                for name in os.listdir(dfd):
                    with contextlib.suppress(OSError):
                        if name.endswith(".json") and \
                                time.time() - os.stat(name, dir_fd=dfd, follow_symlinks=False).st_mtime > HOUR:
                            os.unlink(name, dir_fd=dfd)
            finally:
                os.close(dfd)
        self._deliver()

    def _spool_one(self, dfd: int, name: str, where=(None, None)):
        rid = name[:-len(".json")]
        waiter = uid = pid = None
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=dfd)
            with os.fdopen(fd, "rb") as f:
                st = os.fstat(f.fileno())
                uid = st.st_uid
                if not stat.S_ISREG(st.st_mode):
                    raise Refuse("E_FORMAT", "a spool request is a regular file")
                raw = f.read(MAX_REQUEST + 1)
            if len(raw) > MAX_REQUEST:
                raise Refuse("E_SIZE", f"a request is at most {MAX_REQUEST} bytes")
            pids = holders(st)   # after our own descriptor is closed
            if len(pids) != 1:
                raise Refuse("E_FORMAT", f"spool request held by {len(pids)} processes; need exactly 1")
            pid = pids[0]
            req, body = parse_frame(raw)
            hst = read_stat(pids[0])
            waiter = SpoolWaiter(rid, pids[0], hst[1] if hst else 0, where)
            reply = self.box.handle(pids[0], st.st_uid, req, body, waiter=waiter,
                                    spool=where if where[0] else None)
        except Refuse as e:   # handle() returns its own refusals: these are frame, size and holder faults
            reply = e.reply()
            self._spool_refused(dfd, name, uid, pid, e.code)
        except OSError as e:   # a symlink (O_NOFOLLOW), or the file went away
            reply = {"ok": False, "code": "E_FORMAT", "error": str(e)[:200]}
            self._spool_refused(dfd, name, uid, pid, "E_FORMAT")
        except Exception as e:   # noqa: BLE001
            print(f"agentmail: spool error: {e!r}", file=sys.stderr, flush=True)
            reply = None
        with contextlib.suppress(FileNotFoundError):
            os.unlink(name, dir_fd=dfd)
        if reply and reply.get("waiting"):
            self.spool_waits.add(waiter)
        elif reply:
            self._spool_reply(rid, reply, where)

    def _spool_refused(self, dfd, name, uid, pid, code):
        if uid is None:
            try:
                uid = os.stat(name, dir_fd=dfd, follow_symlinks=False).st_uid
            except OSError:
                uid = self.box.uid
        with contextlib.suppress(Exception):
            self.box.log_refused(uid, code=code, what="frame", req_op="?", pid=pid)

    def _spool_reply(self, rid, reply, where=(None, None)):
        """Written beside the request, in the spool it came from; never through a link a writer planted."""
        try:
            dfd = self._open(where, "rep")
        except (OSError, SpoolUnsafe) as e:
            print(f"agentmail: spool reply {rid} not written: {e}", file=sys.stderr, flush=True)
            return
        try:
            tmp = f"{rid}.tmp"
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp, dir_fd=dfd)
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=dfd)
            with os.fdopen(fd, "w") as f:
                f.write(json.dumps(reply) + "\n")
            os.replace(tmp, f"{rid}.json", src_dir_fd=dfd, dst_dir_fd=dfd)
        except OSError as e:
            print(f"agentmail: spool reply {rid} not written: {e}", file=sys.stderr, flush=True)
        finally:
            os.close(dfd)


def serve(home: Path = HOME, box: Mailbox | None = None, ready: threading.Event | None = None,
          stop: threading.Event | None = None, **kw):
    home = Path(home)
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(home, 0o700)
    lock = lock_home(home)
    if lock is None:
        raise SystemExit(f"agentmail: already running in {home}")
    try:
        box = box or Mailbox(home, journal=lambda s: print(s, flush=True))
        Server(box, home, stop, **kw).run(ready)
    finally:
        lock.close()


# ---- client --------------------------------------------------------------------------------

class NoAnswer(Exception):
    """Sent, but no reply came: the outcome is unknown."""


class NoAnswerInTime(NoAnswer):
    """NoAnswer because the caller's timeout ran out. Every `except NoAnswer` still sees it;
    wait_reply tells it apart so that `wait --max` ends on time."""


def _encode(req: dict, body: bytes | None) -> bytes:
    raw = json.dumps(req).encode() + b"\n"
    if body is not None:
        raw += struct.pack(">I", len(body)) + body
    return raw


def call(req: dict, body: bytes | None = None, home: Path = HOME, timeout: float | None = 15) -> dict:
    """The socket; the spool when a sandbox refuses the connect (Codex: EPERM). Never another daemon."""
    home = Path(home)
    seat_spool = os.environ.get("AGENTMAIL_SPOOL")
    if seat_spool:   # a seat's own spool folder (only where the client writes; the daemon never reads this)
        if not os.path.isabs(seat_spool):
            raise ValueError("AGENTMAIL_SPOOL must be an absolute path")
        return call_spool(req, body, home, timeout, spool=Path(seat_spool))
    if os.environ.get("AGENTMAIL_TRANSPORT") == "spool":
        return call_spool(req, body, home, timeout)
    try:
        return _call_socket(req, body, home, timeout)
    except PermissionError:
        return call_spool(req, body, home, timeout)


def _call_socket(req, body, home, timeout) -> dict:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    with s:
        s.connect(str(home / "sock"))   # errors here: nothing was sent
        try:
            s.sendall(_encode(req, body))
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
        except TimeoutError as e:
            raise NoAnswerInTime(str(e)) from e
        except OSError as e:
            raise NoAnswer(str(e)) from e
    if not buf:
        raise NoAnswer("the connection closed without a reply")
    return json.loads(buf)


def call_spool(req, body=None, home: Path = HOME, timeout: float | None = 15, spool: Path | None = None) -> dict:
    spool = Path(spool) if spool else Path(home) / "spool"
    rid = uuid.uuid4().hex
    part, ready, rep = spool / "req" / f"{rid}.part", spool / "req" / f"{rid}.json", spool / "rep" / f"{rid}.json"
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as f:   # held open until the reply: this open file is our identity
        f.write(_encode(req, body))
        f.flush()
        os.rename(part, ready)
        deadline = None if timeout is None else time.monotonic() + timeout
        while deadline is None or time.monotonic() < deadline:
            try:
                reply = json.loads(rep.read_text())
                rep.unlink(missing_ok=True)
                return reply
            except (OSError, ValueError):
                time.sleep(0.1)
    if ready.exists():
        ready.unlink(missing_ok=True)
        raise ConnectionRefusedError("no reply from agentmail (spool)")   # never picked up: not sent
    raise NoAnswerInTime("no reply from agentmail (spool)")


def call_retrying(req, body=None, home: Path = HOME, timeout: float = 15) -> dict:
    """One run, one request_id: a busy daemon or a lost reply is retried under the same id."""
    lost, rep = None, None
    for attempt in range(6):
        try:
            rep = call(req, body, home, timeout)
        except BlockingIOError:
            pass   # the listen queue is full (AF_UNIX connect with a timeout): nothing was sent
        except NoAnswer as e:
            lost = e
            if "request_id" not in req:
                raise
        else:
            if not rep.get("retry"):
                return rep
        time.sleep(0.2 * (attempt + 1))
    if lost:
        raise lost
    if rep is None:
        raise BlockingIOError("agentmail is busy")
    return rep


def hash_ref(path: str) -> dict:
    """The sender's claim about a file: opened without following links, regular, 64 MiB at most."""
    p = os.path.abspath(os.path.expanduser(path))
    claim = {"path": p, "sha256": None, "size": None}
    try:
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except OSError:
        return claim
    with os.fdopen(fd, "rb") as f:
        st = os.fstat(f.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_size > REF_FILE_MAX:
            return claim
        h, n = hashlib.sha256(), 0
        while chunk := f.read(MIB):
            n += len(chunk)
            if n > REF_FILE_MAX:
                return claim
            h.update(chunk)
    return {"path": p, "sha256": h.hexdigest(), "size": n}


def check_ref(ref: dict, budget: list[int]) -> str:
    """§3.4 ref-check: unchanged, CHANGED since it was sent, or not readable here."""
    if not ref.get("sha256"):
        return "not hashed by the sender: nothing to compare"
    try:
        fd = os.open(ref["path"], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    except OSError as e:
        return "not readable here (symbolic link)" if e.errno == 40 else f"not readable here ({e.strerror})"
    with os.fdopen(fd, "rb") as f:
        st = os.fstat(f.fileno())
        if not stat.S_ISREG(st.st_mode):
            return "not readable here (not a regular file)"
        if st.st_size > REF_FILE_MAX:
            return "not readable here (over 64 MiB)"
        if st.st_size > budget[0]:
            return "not readable here (over this run's 128 MiB)"
        h, n = hashlib.sha256(), 0
        while chunk := f.read(MIB):
            n += len(chunk)
            if n > min(REF_FILE_MAX, budget[0]):
                return "not readable here (grew past the limit)"
            h.update(chunk)
        budget[0] -= n
    return "unchanged" if h.hexdigest() == ref["sha256"] and n == ref.get("size", n) else "CHANGED since it was sent"


def render(m: dict) -> str:
    when = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(m["ts"]))
    out = [f"--- {m['kind']} {m['id']} from {m['from']} [{m['level']}] to {m['to']} at {when}",
           f"stamped by agentmail: account {m['from_account']}, {m['from_kind']} pid {m['from_pid']}, "
           f"{m['bytes']} bytes, sha256 {m['sha256'][:12]}",
           f"state: {m['state']}, {m['fact']}" + (f", answered by {m['answer']}" if m.get("answer") else "")
           + (f", note in the closer's words: {quote(m['note'])}" if m.get("note") else "")]
    if m.get("re"):
        out.append(f"answers request {m['re']}")
    out += [f"subject in the sender's words: {quote(m['subject'])}", NOT_APPROVAL]
    if m.get("body") is None:
        out.append("| (body deleted: retention or expiry)")
    else:
        out += ["| " + line for line in clean(m["body"]).split("\n")]
    for r in m.get("refs") or []:
        claim = (f"sender's sha256 {r['sha256'][:12]}, {size(r.get('size'))}" if r.get("sha256") else "not hashed")
        out.append(f"path given by the sender, not opened: {clean(r['path'], False)} ({claim})")
    return "\n".join(out)


def _err(rep: dict) -> int:
    print(f"agentmail: {rep.get('code', 'E_FORMAT')}: {clean(str(rep.get('error')), False)}", file=sys.stderr)
    return 1


def _unreachable(e) -> int:
    print(f"agentmail: daemon not reachable ({e}); is agentmail.service running?", file=sys.stderr)
    return 2


def hook(event: str, home: Path = HOME) -> int:
    """§4.1: at most one block, always exit 0, 2 seconds of its own."""
    if event == "guard":
        return guard(sys.stdin)
    if event not in ("session-start", "prompt"):
        return 0
    start = event == "session-start"

    def budget(*_):
        raise TimeoutError("hook budget")
    signal.signal(signal.SIGALRM, budget)
    signal.setitimer(signal.ITIMER_REAL, HOOK_BUDGET)
    try:
        with contextlib.suppress(Exception):   # the engine's hook input is not needed; drain it briefly
            if select.select([sys.stdin], [], [], 0.05)[0]:
                sys.stdin.buffer.read1(1 << 20)
        held_until = time.monotonic() + 1.0   # E_HELD: a restart's old process may still be exiting
        while True:
            rep = call({"op": "hook", "event": event}, home=home, timeout=1.5)
            if rep.get("code") == "E_HELD" and time.monotonic() < held_until:
                time.sleep(0.1)
                continue
            break
    except Exception:   # noqa: BLE001 - daemon down, slow or broken: say so once, at start only
        signal.setitimer(signal.ITIMER_REAL, 0)
        if start:
            print("agentmail: not reachable")
        return 0
    signal.setitimer(signal.ITIMER_REAL, 0)
    if rep.get("ok") and rep.get("block"):
        print(rep["block"])
    elif rep.get("code") == "E_HELD" and start:
        print(f"agentmail: seat {rep.get('seat')} is held by pid {rep.get('holder')}")
    return 0


AGENTMAIL_PROGRAMS = {"agentmail", "agentmail.py"}
# Words after which the next words are a command again; options before that command are skipped.
GUARD_WRAPPERS = {"env", "command", "exec", "nohup", "time", "sudo", "doas", "xargs", "nice", "ionice", "timeout",
                  "setsid", "stdbuf", "chrt", "taskset", "strace", "ltrace", "flock", "watch", "unbuffer", "su",
                  "script", "builtin", "coproc", "source", "."}
GUARD_KEYWORDS = {"!", "{", "}", "if", "then", "else", "elif", "do", "while", "until"}
GUARD_SHELLS = {"sh", "bash", "dash", "zsh", "ksh", "mksh", "fish", "ash", "busybox"}
GUARD_PYTHON = re.compile(r"(?:python|pypy)[0-9.]*")
GUARD_ASSIGN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\+?=(.*)", re.S)


def _guard_words(command: str) -> list[list[str]]:
    """Shell words grouped into segments: split at ; & | ( ) and newlines; redirect targets are dropped."""
    import shlex
    lex = shlex.shlex(command, posix=True, punctuation_chars=";&|()<>\n")
    lex.whitespace, lex.whitespace_split, lex.commenters = " \t\r", True, ""
    segs, cur, redirect = [], [], None
    for tok in lex:
        if tok and all(c in ";&|()<>\n" for c in tok):
            if any(c in ";&|()\n" for c in tok):
                segs.append(cur)
                cur, redirect = [], None
            else:
                redirect = tok
            continue
        if redirect is not None:
            if redirect == "<<<":
                segs.append(["<<<", tok])   # a here-string may feed a shell: checked as a command line
            redirect = None
            continue
        cur.append(tok)
    segs.append(cur)
    return segs


def _named_anywhere(s: str) -> bool:
    """The old, coarse test: any word at all named like the program. Used when in doubt."""
    return any(os.path.basename(t) in AGENTMAIL_PROGRAMS for t in re.split(r"[\s;&|()<>`'\"$={}]+", s))


def _calls(words: list[str], depth: int) -> bool:
    """Does this simple command (its words) run agentmail?"""
    if depth > 8:
        return _named_anywhere(" ".join(words))
    i = 0
    while i < len(words) and (words[i] in GUARD_KEYWORDS or words[i] == "function" or GUARD_ASSIGN.fullmatch(words[i])):
        i += 2 if words[i] == "function" else 1
    if i >= len(words):
        return False
    w, rest = words[i], words[i + 1:]
    base = os.path.basename(w)
    if base in AGENTMAIL_PROGRAMS:
        return True
    if w == "<<<" or base == "eval":
        return runs_agentmail(" ".join(rest), depth + 1)
    if base in GUARD_WRAPPERS or base in GUARD_SHELLS:
        j = 0
        while j < len(rest) and (rest[j].startswith("-") or GUARD_ASSIGN.fullmatch(rest[j])):
            j += 1
        if base in GUARD_SHELLS:
            if any(re.fullmatch(r"-[A-Za-z]*c[A-Za-z]*", o) for o in rest):   # -c: the body (and argv[0])
                return any(runs_agentmail(a, depth + 1) for a in rest if not a.startswith("-"))
            # a script: the first operand, or the second when an option took the first as its argument
            return any(os.path.basename(a) in AGENTMAIL_PROGRAMS for a in rest[j:j + 2])
        if any(runs_agentmail(a, depth + 1) for a in rest if any(c.isspace() for c in a)):
            return True   # a quoted command line (env -S, su -c, watch, script -c, flock -c)
        # the command is the first operand, or the second when an option or a duration took the first
        return _calls(rest[j:], depth + 1) or _calls(rest[j + 1:], depth + 1)
    if GUARD_PYTHON.fullmatch(base):
        j = 0
        while j < len(rest) and rest[j].startswith("-") and rest[j] != "-":
            o = rest[j]
            if re.fullmatch(r"-[bBdEhiIOPqsSuvx]*[cm]", o):
                arg = rest[j + 1] if j + 1 < len(rest) else ""
                return (o.endswith("m") and arg.split(".")[0] in {"agentmail"}) or \
                    (o.endswith("c") and any(t in AGENTMAIL_PROGRAMS for t in re.split(r"[^\w.-]+", arg)))
            j += 2 if o in ("-W", "-X", "--check-hash-based-pycs") else 1
        return j < len(rest) and os.path.basename(rest[j]) in AGENTMAIL_PROGRAMS
    for k, a in enumerate(rest):   # find ... -exec CMD ;
        if a in ("-exec", "-execdir", "-ok", "-okdir") and _calls(rest[k + 1:], depth + 1):
            return True
    return False


def runs_agentmail(command: str, depth: int = 0) -> bool:
    """An accident guard, not a boundary: True when a shell command line would run agentmail.
    Only command words count (after assignments and wrappers such as env, sudo, xargs), plus a Python
    interpreter's script and `sh -c` bodies, so `cd ~/src/agentmail` passes. Anything it cannot parse,
    and anything nested too deeply, falls back to blocking on any word named like the program."""
    if depth > 8:
        return _named_anywhere(command)
    named = _named_anywhere(command)
    try:
        for seg in _guard_words(command):
            if _calls(seg, depth):
                return True
            for k, wd in enumerate(seg if named else ()):   # python3 < agentmail.py, cat agentmail.py | bash
                b = os.path.basename(wd)
                if GUARD_PYTHON.fullmatch(b) or b in GUARD_SHELLS:
                    op = next((a for a in seg[k + 1:] if a == "-" or not a.startswith("-")), "-")
                    if op == "-":   # no script operand: the program text comes from stdin
                        return True
            if "$" in command or (seg and seg[0] == "alias"):   # X=agentmail; $X ... : the value may run
                for wd in seg:
                    m = GUARD_ASSIGN.fullmatch(wd)
                    if m and runs_agentmail(m.group(1), depth + 1):
                        return True
        if "$(" in command or "`" in command:   # substitutions run even inside double quotes
            flat = re.sub(r"\$\(|`", " ; ", command.replace("'", " ").replace('"', " "))
            return any(_calls(seg, depth) for seg in _guard_words(flat))
    except ValueError:   # unbalanced quotes or a trailing backslash
        return named
    return False


def guard(stdin) -> int:
    """§7: purely local, never connects. Exit 2 blocks one subagent command that runs agentmail."""
    try:
        if not select.select([stdin], [], [], 1.0)[0]:
            return 0
        data = json.loads(stdin.read(1 << 20))
        if isinstance(data, dict) and data.get("agent_id"):
            cmd = (data.get("tool_input") or {}).get("command")
            if isinstance(cmd, str) and runs_agentmail(cmd):
                print(GUARD_MSG, file=sys.stderr)
                return 2
    except Exception:   # noqa: BLE001 - unreadable input never blocks anything
        return 0
    return 0


def bare_wait() -> int:
    """The old `wait`: the notice, then sleep until the session (or our parent) goes away.
    No connection: a background command that returns would start a turn."""
    print(NOTICE_WAIT, flush=True)
    ppid = os.getppid()
    target = nearest_engine(ppid) or ppid
    try:
        fd = os.pidfd_open(target)
    except OSError:
        return 0
    while True:
        if select.select([fd], [], [], 1.0)[0] or os.getppid() != ppid:
            return 0


def unreachable_line(seconds: float) -> str:
    n = f"{seconds / 60:g} minutes" if seconds >= 60 else f"{seconds:g} seconds"
    return f"agentmail: the mailbox has been unreachable for {n}; this wait has ended"


def wait_reply(msg_id: str, max_minutes: float | None, home: Path = HOME,
               give_up: float | None = None) -> int:
    """Reconnects through a daemon restart; ends (exit 2) after 10 minutes with no mailbox at all."""
    give_up = WAIT_UNREACHABLE if give_up is None else give_up
    deadline = None if max_minutes is None else time.monotonic() + max_minutes * 60
    down_since = None
    while True:
        left = None if deadline is None else deadline - time.monotonic()
        if left is not None and left <= 0:
            print("pending")
            return 0
        try:
            rep = call({"op": "wait", "msg": msg_id}, home=home, timeout=left)
        except (socket.timeout, TimeoutError, NoAnswerInTime):
            continue   # the deadline check above answers
        except (FileNotFoundError, ConnectionRefusedError):   # no mailbox listening
            down_since = down_since or time.monotonic()
            if time.monotonic() - down_since >= give_up:
                print(unreachable_line(give_up), file=sys.stderr)
                return 2
            left = None if deadline is None else max(0.0, deadline - time.monotonic())
            time.sleep(min(1.0, give_up) if left is None else min(1.0, left, give_up))
            continue
        except (OSError, NoAnswer, ValueError):
            down_since = None   # it answered the connect: a restart in progress
            left = None if deadline is None else max(0.0, deadline - time.monotonic())
            time.sleep(1.0 if left is None else min(1.0, left))
            continue
        down_since = None
        if rep.get("retry"):
            time.sleep(1.0)
            continue
        if not rep.get("ok"):
            return _err(rep)
        print(rep["done"])
        return 0


def doctor(accept_clock: bool, home: Path = HOME, settings: Path = SETTINGS) -> int:
    fails, lines = 0, []

    def check(ok, what):
        nonlocal fails
        fails += not ok
        lines.append(f"{'PASS' if ok else 'FAIL'}  {what}")

    sock = home / "sock"
    try:
        st, hst = sock.lstat(), home.stat()
        check(stat.S_ISSOCK(st.st_mode) and st.st_uid == os.getuid() and st.st_mode & 0o777 == 0o600
              and hst.st_mode & 0o777 == 0o700, f"socket {sock}: owner uid {st.st_uid}, mode "
              f"{st.st_mode & 0o777:o}, directory mode {hst.st_mode & 0o777:o}")
    except OSError as e:
        check(False, f"socket {sock}: {e.strerror}")
    rep = None
    try:
        if accept_clock:
            r = call({"op": "accept_clock"}, home=home, timeout=3)
            check(r.get("ok"), "clock accepted" if r.get("ok") else f"accept clock: {r.get('error')}")
        rep = call({"op": "doctor"}, home=home, timeout=3)
        check(rep.get("ok"), "the daemon answers" + ("" if rep.get("ok") else f": {rep.get('error')}"))
    except (OSError, NoAnswer, ValueError) as e:
        check(False, f"the daemon answers: {e}")
    if rep and rep.get("ok"):
        check(rep["unit"] == UNIT, f"unit: {rep['unit'] or 'not under systemd'} (want {UNIT})")
        check(rep["prefix"] == JOURNAL_PREFIX, f"journal prefix {rep['prefix']!r}")
        r = rep["roster"]
        check(r["ok"], f"roster {r['path']} loads" + ("" if r["ok"] else f": {r['error']} (using the last good copy)"))
        check(store_ok(rep), f"store {size(rep['store_bytes'])} of {size(rep['store_cap'])}")
        check(rep["clock"] == "ok", f"clock: {rep['clock']}")
    try:
        hooks = json.loads(settings.read_text()).get("hooks", {})
        for event, cmd in (("SessionStart", "agentmail hook session-start"), ("UserPromptSubmit", "agentmail hook prompt"),
                           ("PreToolUse", "agentmail hook guard")):
            found = any(cmd in str(h.get("command", "")) for e in hooks.get(event, []) for h in e.get("hooks", []))
            check(found, f"hook entry {event} -> {cmd} in {settings}")
    except (OSError, ValueError, AttributeError) as e:
        check(False, f"hook entries in {settings}: {e}")
    if rep and rep.get("ok"):
        lines.append(f"note  level of this account's stamps: {rep['level']}")
        lines.append("note  engine processes without a lease (a restarted chat has none until prompted): "
                     + (", ".join(rep["unleased_engines"]) or "none"))
        lines.append("note  seats never leased: " + (", ".join(rep["never_leased"]) or "none"))
        lines.append("note  old names called in the last two weeks: "
                     + (", ".join(f"{k} ({v})" for k, v in sorted(rep["aliases"].items())) or "none")
                     + "; bare wait makes no connection and is not counted")
    print("\n".join(lines))
    return 3 if fails else 0


# ---- commands ------------------------------------------------------------------------------

def _rid(given: str | None) -> str:
    if given and not RID_RE.fullmatch(given):
        raise SystemExit("agentmail: --request-id is 32 hex characters")
    return given or uuid.uuid4().hex


def _changing(req, body=None, home=HOME) -> dict | int:
    try:
        return call_retrying(req, body, home)
    except NoAnswer as e:
        print(f"agentmail: no answer ({e}); the outcome is unknown. Repeat with --request-id {req['request_id']}",
              file=sys.stderr)
        return 2
    except (OSError, ValueError) as e:
        return _unreachable(e)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["hook"]:   # the engine's hook: never an error, whatever the arguments
        try:
            return hook(argv[1] if len(argv) > 1 else "", HOME)
        except BaseException:   # noqa: BLE001
            return 0
    ap = argparse.ArgumentParser(prog="agentmail", description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("send")
    s.add_argument("to")
    s.add_argument("--kind", required=True)
    s.add_argument("--subject")
    s.add_argument("--re")
    s.add_argument("--more", action="store_true")
    s.add_argument("--ref", action="append", default=[])
    s.add_argument("--ttl", type=int)
    s.add_argument("--file")
    s.add_argument("--request-id")
    s.add_argument("--again", action="store_true")
    i = sub.add_parser("inbox")
    i.add_argument("--all", action="store_true")
    sub.add_parser("read").add_argument("id")
    rc = sub.add_parser("ref-check")
    rc.add_argument("id")
    rc.add_argument("n", nargs="?", type=int)
    a = sub.add_parser("ack")
    a.add_argument("ids", nargs="+")
    a.add_argument("--request-id")
    sub.add_parser("out")
    c = sub.add_parser("close")
    c.add_argument("id")
    c.add_argument("--declined", action="store_true")
    c.add_argument("--note")
    c.add_argument("--request-id")
    w = sub.add_parser("wait")
    w.add_argument("--re")
    w.add_argument("--max", type=float)
    w.add_argument("--timeout", type=float, help=argparse.SUPPRESS)   # old flag, ignored
    sub.add_parser("seats")
    sub.add_parser("desk")
    sub.add_parser("register").add_argument("name")
    d = sub.add_parser("doctor")
    d.add_argument("--accept-clock", action="store_true")
    sub.add_parser("serve")
    v = sub.add_parser("recv")   # old name: inbox, then read each item
    v.add_argument("--peek", action="store_true")
    v.add_argument("--json", action="store_true")
    sub.add_parser("who")        # old name: seats
    args = ap.parse_args(argv)
    home = HOME

    if args.cmd == "serve":
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        serve(home, stop=stop)
        return 0
    if args.cmd == "wait" and not args.re:
        return bare_wait()
    if args.cmd == "wait":
        return wait_reply(args.re, args.max, home)
    if args.cmd == "doctor":
        return doctor(args.accept_clock, home)
    if args.cmd == "ref-check":
        try:
            rep = call({"op": "refs", "msg": args.id}, home=home)
        except (OSError, NoAnswer, ValueError) as e:
            return _unreachable(e)
        if not rep.get("ok"):
            return _err(rep)
        refs = rep["refs"]
        if args.n is not None and not 1 <= args.n <= len(refs):
            print(f"agentmail: {args.id} has {len(refs)} references", file=sys.stderr)
            return 1
        budget = [REF_RUN_MAX]
        for n, r in enumerate(refs, 1):
            if args.n in (None, n):
                print(f"{n} {clean(r['path'], False)}: {check_ref(r, budget)}")
        return 0

    try:
        if args.cmd == "send":
            if args.kind not in KINDS:
                print("agentmail: E_KIND: kind must be request, report or info (no message is an approval)",
                      file=sys.stderr)
                return 1
            body = Path(args.file).read_bytes() if args.file else sys.stdin.buffer.read()
            req = {"op": "send", "to": args.to, "kind": args.kind, "refs": [hash_ref(p) for p in args.ref],
                   "request_id": _rid(args.request_id)}
            for k, val in (("subject", args.subject), ("re", args.re), ("ttl_days", args.ttl)):
                if val is not None:
                    req[k] = val
            if args.more:
                req["more"] = True
            if args.again:
                req["again"] = True
            rep = _changing(req, body, home)
            if isinstance(rep, int):
                return rep
            if rep.get("ok"):
                print(f"not sent again: identical to {rep['id']}, which you sent {rep['min_ago']} min ago. "
                      "Use --again to send another." if rep.get("already") else f"sent {rep['id']}: {rep['note']}")
        elif args.cmd == "ack":
            rep = _changing({"op": "ack", "msgs": args.ids, "request_id": _rid(args.request_id)}, None, home)
            if isinstance(rep, int):
                return rep
            if rep.get("ok"):
                print(f"acked {len(rep['acked'])}" + (f"; already acked: {' '.join(rep['already'])}" if rep["already"]
                                                     else ""))
        elif args.cmd == "close":
            req = {"op": "close", "msg": args.id, "request_id": _rid(args.request_id)}
            if args.declined:
                req["declined"] = True
            if args.note is not None:
                req["note"] = args.note
            rep = _changing(req, None, home)
            if isinstance(rep, int):
                return rep
            if rep.get("ok"):
                print(f"{rep['id']}: {rep['state']}")
        elif args.cmd in ("inbox", "recv"):
            req = {"op": "inbox"} if args.cmd == "recv" else {"op": "inbox", "all": args.all}
            if args.cmd == "recv":
                req["alias"] = "recv"
            rep = call(req, home=home)
            if rep.get("ok") and args.cmd == "recv" and not args.peek:
                for it in rep["items"]:
                    r2 = call({"op": "read", "msg": it["id"]}, home=home)
                    print(render(r2["msg"]) + "\n" if r2.get("ok") else f"agentmail: {r2.get('error')}")
                print(f"({len(rep['items'])} message(s) for {rep['seat']}; acknowledge with agentmail ack <id>)")
            elif rep.get("ok"):
                for it in rep["items"]:
                    print(f"{header_line(it)}   {it['state']}, {it['age']}, {it['fact']}")
                if not rep["items"]:
                    print(f"agentmail ({rep['seat']}): nothing waiting")
        elif args.cmd == "read":
            rep = call({"op": "read", "msg": args.id}, home=home)
            if rep.get("ok"):
                print(render(rep["msg"]))
        elif args.cmd == "out":
            rep = call({"op": "out"}, home=home)
            if rep.get("ok"):
                for it in rep["items"]:
                    extra = f", stuck: {it['stuck']}" if it.get("stuck") else ""
                    print(f"{it['kind']:<7} {it['id']} to {it['to']:<14} {it['age']:>3}   {it['state']}, "
                          f"{it['fact']}{extra}")
        elif args.cmd in ("seats", "who"):
            req = {"op": "seats", **({"alias": "who"} if args.cmd == "who" else {})}
            rep = call(req, home=home)
            if rep.get("ok"):
                print(f"you: {rep['you'] or '(no seat)'}")
                for st in rep["seats"]:
                    print(f"{st['name']:<20} {st['status']}" + (f"   [{', '.join(st['notes'])}]" if st["notes"] else ""))
        elif args.cmd == "desk":
            rep = call({"op": "desk"}, home=home)
            if rep.get("ok"):
                print("\n".join(rep["lines"]) or "desk: nothing open")
        elif args.cmd == "register":
            rep = call({"op": "register", "name": args.name}, home=home)
            if rep.get("ok"):
                print("binding is automatic" if rep.get("automatic") else f"registered {rep['seat']}")
    except (OSError, NoAnswer, ValueError) as e:
        return _unreachable(e)
    return 0 if rep.get("ok") else _err(rep)


if __name__ == "__main__":
    sys.exit(main())
