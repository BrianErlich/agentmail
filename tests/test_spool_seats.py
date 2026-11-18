"""Spool-folder seats (DESIGN.md §8): a second Codex seat,
chosen by the spool folder a request is written in, never by anything the caller says.
Temporary homes only; real daemons (threads) and stub engine processes, as in StubEngineTest."""
import json
import os
import shutil
import subprocess
import sys
import time
import unittest
import uuid
from pathlib import Path

from tests import test_agentmail as t2

am, REPO, STUB = t2.am, t2.REPO, t2.STUB
SEAT = "codex-second-seat"
RAW = ("import json, sys; sys.path.insert(0, sys.argv[1]); import agentmail as am; "
       "print(json.dumps(am.call(json.loads(sys.argv[2]), timeout=5)))")


def roster_with(*seats: str) -> str:
    return t2.ROSTER + "".join(seats)


def spool_seat(name, path, extra=""):
    return f'\n[seat.{name}]\nengine = "codex"\nspool = "{path}"\n{extra}'


class SpoolRosterTest(unittest.TestCase):
    """Rule 1: each roster rule accepts and refuses."""

    def ok(self, text, home=None):
        return am.parse_roster(text, home)

    def bad(self, text, home=None):
        with self.assertRaises(ValueError, msg=text):
            am.parse_roster(text, home)

    def test_rules(self):
        good = spool_seat(SEAT, "/srv/x/spool", 'overview = true\nreads = "on request"\nretired = false\n')
        self.assertEqual(self.ok(good)[SEAT], {"engine": "codex", "spool": "/srv/x/spool", "overview": True,
                                               "reads": "on request", "retired": False})
        self.assertEqual(self.ok('[seat.codex]\nengine = "codex"\n'), {"codex": {"engine": "codex"}})
        self.bad('[seat.a1]\nengine = "claude"\n')                                 # only "codex"
        self.bad('[seat.a1]\nengine = 1\n')
        self.bad('[seat.a1]\nspool = "/srv/x"\n')                                   # spool needs engine = codex
        self.bad('[seat.a1]\nengine = "claude"\nspool = "/srv/x"\n')
        self.bad('[seat.a1]\nengine = "codex"\nspool = 5\n')
        for p in ("srv/x", "./x", "/srv/../x", "/srv/x/", "//srv/x", "/srv//x", "/srv/./x", "/", "",
                  "/srv/x\\u0000y", "/srv/x\\u001by", "/" + "a" * 1100):
            self.bad(spool_seat("a1", p))
        self.ok(spool_seat("a1", "/srv/x.y/sp-ool_2"))
        self.bad('[seat.codex]\nengine = "codex"\nspool = "/srv/x"\n')            # never the seat `codex`
        for k in ('names = ["b1"]', 'where = "mac"', "account = 4242"):          # only the five keys
            self.bad(spool_seat("a1", "/srv/x", k + "\n"))
        self.bad(spool_seat("a1", "/srv/x") + spool_seat("b1", "/srv/x"))         # one path, one seat
        self.ok(spool_seat("a1", "/srv/x") + spool_seat("b1", "/srv/x/y"))
        with t2.tmpdir() as d:
            home = Path(d) / "v2"
            (home / "spool").mkdir(parents=True)
            shared = str(home / "spool")
            for p in (shared, shared + "/req", shared + "/rep/x"):
                self.ok(spool_seat("a1", p))                                       # parse alone: no home
                self.bad(spool_seat("a1", p), home)
            self.ok(spool_seat("a1", shared + "x"), home)                         # a sibling, not inside
            self.ok(spool_seat("a1", str(home / "other")), home)
            link = Path(d) / "link"
            link.symlink_to(home)
            self.bad(spool_seat("a1", shared), link)                              # the real path counts too

    def test_mailbox_keeps_the_last_good_roster(self):
        with t2.tmpdir() as d:
            root = Path(d)
            roster = root / "roster.toml"
            roster.write_text(t2.ROSTER)
            lines = []
            box = am.Mailbox(root / "v2", lambda pid: {}, lambda i: True, journal=lines.append, roster=roster)
            try:
                good, gen = dict(box.roster), box.roster_gen
                roster.write_text(spool_seat("a1", str(root / "v2" / "spool" / "x")))
                os.utime(roster, ns=(time.time_ns() + 10**9,) * 2)
                box.roster_check()
                self.assertIn("inside the shared spool", box.roster_err)
                self.assertEqual((box.roster, box.roster_gen), (good, gen))
                roster.write_text(spool_seat("a1", str(root / "ws" / "spool")))
                os.utime(roster, ns=(time.time_ns() + 2 * 10**9,) * 2)
                box.roster_check()
                self.assertIsNone(box.roster_err)
                self.assertEqual(box.roster_gen, gen + 1)
            finally:
                box.db.close()


class SeatDirTest(unittest.TestCase):
    """Rule 2, the directory checks, including the owner check that needs another uid."""

    def setUp(self):
        self.tmp = t2.tmpdir()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_checks(self):
        sp = self.root / "ws" / "spool"
        sp.mkdir(parents=True, mode=0o700)
        os.close(am.open_seat_dir(str(sp), "req", os.getuid()))
        self.assertEqual(oct((sp / "req").stat().st_mode & 0o777), oct(0o700))   # created, mode 700
        with self.assertRaisesRegex(am.SpoolUnsafe, "owned by uid"):
            am.open_seat_dir(str(sp), "req", os.getuid() + 1)
        with self.assertRaises(FileNotFoundError):
            am.open_seat_dir(str(self.root / "missing"), "req", os.getuid())
        self.assertFalse((self.root / "missing").exists())                         # never created
        (self.root / "link").symlink_to(sp)
        with self.assertRaisesRegex(am.SpoolUnsafe, "symbolic link"):
            am.open_seat_dir(str(self.root / "link"), "req", os.getuid())
        (self.root / "up").symlink_to(self.root / "ws")                            # a link above the spool
        with self.assertRaisesRegex(am.SpoolUnsafe, "symbolic link"):
            am.open_seat_dir(str(self.root / "up" / "spool"), "req", os.getuid())
        shutil.rmtree(sp / "req")
        (sp / "req").symlink_to(self.root)                                         # a planted req link
        with self.assertRaisesRegex(am.SpoolUnsafe, "symbolic link"):
            am.open_seat_dir(str(sp), "req", os.getuid())
        (sp / "req").unlink()
        os.chmod(sp, 0o720)
        with self.assertRaisesRegex(am.SpoolUnsafe, "writable by group"):
            am.open_seat_dir(str(sp), "req", os.getuid())
        os.chmod(sp, 0o700)
        (sp / "rep").mkdir(mode=0o700)
        os.chmod(sp / "rep", 0o702)
        with self.assertRaisesRegex(am.SpoolUnsafe, "writable by group"):
            am.open_seat_dir(str(sp), "rep", os.getuid())


class SpoolSeatBase(t2.CliBase):
    roster_extra = 'overview = true\nreads = "on request"\n'
    server_kw: dict = {}

    def setUp(self):
        self.tmp = t2.tmpdir()
        self.root = Path(self.tmp.name)
        self.home = self.root / "v2"
        self.ws = self.root / "codex-second-seat"
        self.sp = self.ws / "spool"
        self.sp.mkdir(parents=True, mode=0o700)
        self.roster = self.root / "roster.toml"
        self.roster.write_text(self.roster_text())
        self.settings = self.root / "settings.json"
        self.lines, self.stubs, self.daemon = [], [], None
        self.start()

    def roster_text(self):
        return roster_with(spool_seat(SEAT, self.sp, self.roster_extra))

    def start(self, **kw):
        self.box = self.make_box(**kw)
        self.daemon = t2.Daemon(self.home, self.box, **self.server_kw)

    def seat_env(self, path=None):
        return self.env(AGENTMAIL_SPOOL=str(path or self.sp))

    def shared_env(self):
        return self.env(AGENTMAIL_TRANSPORT="spool")

    def raw(self, stub, req, env):
        r = self.ctl(stub, argv=[sys.executable, "-I", "-c", RAW, str(REPO), json.dumps(req)], env=env, input="")
        self.assertEqual(r["rc"], 0, r)
        return json.loads(r["out"])

    def helper(self):
        """A Codex helper process that is not the listening app-server."""
        d = self.root / f"codex-helper-{len(self.stubs)}"
        d.mkdir()
        (d / "exec").write_text(Path(STUB).read_text())
        s = subprocess.Popen(["codex", "exec", "--json"], executable=sys.executable, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True, cwd=d)
        self.stubs.append(s)
        return s

    def refused(self, what=None):
        return [e for e in self.events("refused") if what is None or e.get("what") == what]

    def leases(self):
        with self.store() as db:
            return {r["seat"]: r["pid"] for r in db.execute("SELECT seat, pid FROM lease")}

    def seat_rows(self):
        with self.store() as db:
            return {r[0] for r in db.execute("SELECT name FROM seat")}

    def reload(self, text):
        self.roster.write_text(text)
        os.utime(self.roster, ns=(time.time_ns() + 10**9,) * 2)
        self.run_cli("seats")                       # any request reloads a changed roster
        time.sleep(0.6)                             # and the next spool passes see it

    def wait_for(self, cond, s=5.0):
        t0 = time.monotonic()
        while time.monotonic() - t0 < s:
            if cond():
                return True
            time.sleep(0.05)
        return False

    def plant(self, spool: Path):
        """A request file in `spool`'s req folder, held open by this test process; the caller closes it."""
        (spool / "req").mkdir(mode=0o700, exist_ok=True)
        os.chmod(spool / "req", 0o700)               # whatever the umask
        rid = uuid.uuid4().hex
        p = spool / "req" / f"{rid}.json"
        f = open(p, "wb")
        f.write(b'{"op": "inbox"}\n')
        f.flush()
        return p, f


class SpoolSeatEndToEndTest(SpoolSeatBase):
    def test_one_process_two_seats(self):
        codex = self.engine(None, kind="codex")
        alpha = self.engine("alpha")
        r = self.do(codex, "seats", env=self.seat_env())
        self.assertEqual(r["rc"], 0, r)
        self.assertIn(f"you: {SEAT}", r["out"])
        r = self.do(codex, "seats", env=self.shared_env())
        self.assertIn("you: codex", r["out"])
        self.assertEqual(self.leases(), {SEAT: codex.pid, "codex": codex.pid})
        out = self.do(alpha, "seats")["out"]
        self.assertIn(f"{SEAT:<20} running (codex pid {codex.pid})", out)
        self.assertIn(f"{'codex':<20} running (codex pid {codex.pid})", out)
        reg = {e["name"]: e for e in self.events("register")}
        new = {e["name"]: e for e in self.events("seat_new")}
        self.assertIs(reg[SEAT]["spool_seat"], True)
        self.assertIs(new[SEAT]["spool_seat"], True)
        self.assertEqual((reg[SEAT]["level"], reg[SEAT]["kind"], reg[SEAT]["pid"]), ("conv", "codex", codex.pid))
        self.assertNotIn("spool_seat", reg["codex"])
        self.assertNotIn("spool_seat", new["codex"])
        for e in (reg[SEAT], new[SEAT]):                    # the legacy fields, with their types
            for k in ("name", "kind", "pid", "account", "uid"):
                self.assertIn(k, e)
        # mail between the two seats of one process; each sees only its own inbox
        r = self.do(codex, "send", "codex", "--kind", "info", "--subject", "to reviewer", inp="a", env=self.seat_env())
        self.assertEqual(r["rc"], 0, r)
        m1 = r["out"].split()[1].rstrip(":")
        r = self.do(codex, "send", SEAT, "--kind", "info", "--subject", "to the second seat", inp="b",
                    env=self.shared_env())
        self.assertEqual(r["rc"], 0, r)
        self.assertIn(f"kept: {SEAT} reads on request", r["out"])
        m2 = r["out"].split()[1].rstrip(":")
        sends = {e["id"]: e for e in self.events("send")}
        self.assertEqual((sends[m1]["from"], sends[m1]["to"]), (SEAT, "codex"))
        self.assertEqual((sends[m2]["from"], sends[m2]["to"]), ("codex", SEAT))
        self.assertEqual((sends[m1]["level"], sends[m2]["level"]), ("conv", "conv"))
        a = self.do(codex, "inbox", env=self.seat_env())["out"]
        b = self.do(codex, "inbox", env=self.shared_env())["out"]
        self.assertIn(m2, a)
        self.assertNotIn(m1, a)
        self.assertIn(m1, b)
        self.assertNotIn(m2, b)
        self.assertIn(f"from codex [conv]", a)
        self.assertIn(f"from {SEAT} [conv]", b)
        self.assertIn("| a", self.do(codex, "read", m1, env=self.seat_env())["out"])   # its own sent item
        m0 = self.do(alpha, "send", "codex", "--kind", "info", inp="c")["out"].split()[1].rstrip(":")
        r = self.do(codex, "read", m0, env=self.seat_env())                  # mail to `codex` is not its own
        self.assertEqual(r["rc"], 1)
        self.assertIn("E_PARTY", r["err"])
        self.assertIn("| c", self.do(codex, "read", m0, env=self.shared_env())["out"])
        self.assertIn("| b", self.do(codex, "read", m2, env=self.seat_env())["out"])
        self.assertIn("acked 1", self.do(codex, "ack", m2, env=self.seat_env())["out"])
        # the desk: the seat with overview has it; `codex` does not
        r = self.do(codex, "desk", env=self.seat_env())
        self.assertEqual(r["rc"], 0, r)
        r = self.do(codex, "desk", env=self.shared_env())
        self.assertEqual(r["rc"], 1)
        self.assertIn("E_OWNER", r["err"])
        # a request to the second seat, waited on, answered with --re, closed, and out
        q = self.do(alpha, "send", SEAT, "--kind", "request", "--subject", "plan?", inp="q")["out"]
        self.assertIn(f"kept: {SEAT} reads on request", q)
        q = q.split()[1].rstrip(":")
        w = self.do(alpha, "wait", "--re", q, bg=True)["pid"]
        time.sleep(0.4)
        self.assertIn(q, self.do(codex, "hook", "prompt", env=self.seat_env())["out"])
        self.assertIn(q, self.do(codex, "desk", env=self.seat_env())["out"])
        r = self.do(codex, "send", "alpha", "--kind", "report", "--re", q, inp="plan", env=self.seat_env())
        self.assertEqual(r["rc"], 0, r)
        self.assertIn(f"request {q} to {SEAT}: answered by report", self.ctl(alpha, collect=w, timeout=10)["out"])
        q2 = self.do(alpha, "send", SEAT, "--kind", "request", inp="q2")["out"].split()[1].rstrip(":")
        self.assertIn("declined", self.do(codex, "close", q2, "--declined", "--note", "no", env=self.seat_env())["out"])
        q3 = self.do(codex, "send", "alpha", "--kind", "request", inp="q3", env=self.seat_env())["out"]
        q3 = q3.split()[1].rstrip(":")
        self.assertIn(q3, self.do(codex, "out", env=self.seat_env())["out"])
        self.assertNotIn(q3, self.do(codex, "out", env=self.shared_env())["out"])
        w = self.do(codex, "wait", "--re", q3, bg=True, env=self.seat_env())["pid"]
        time.sleep(0.5)
        self.assertEqual(self.do(alpha, "send", SEAT, "--kind", "report", "--re", q3, inp="r")["rc"], 0)
        self.assertIn("answered", self.ctl(codex, collect=w, timeout=10)["out"])
        # retries: the same request id over the seat spool is the same send
        rid = uuid.uuid4().hex
        r1 = self.do(codex, "send", "alpha", "--kind", "info", "--request-id", rid, inp="x", env=self.seat_env())
        r2 = self.do(codex, "send", "alpha", "--kind", "info", "--request-id", rid, inp="x", env=self.seat_env())
        self.assertEqual(r1["out"], r2["out"])
        self.assertEqual(list((self.sp / "req").iterdir()), [])
        self.assertEqual(list((self.sp / "rep").iterdir()), [])
        # the engine ends: both of its leases end
        codex.kill()
        codex.wait()
        self.assertTrue(self.wait_for(lambda: {e["name"] for e in self.events("lease_end")} >= {"codex", SEAT}))
        ends = [e for e in self.events("lease_end") if e["name"] in ("codex", SEAT)]
        self.assertEqual(sorted(e["name"] for e in ends), ["codex", SEAT])
        self.assertEqual({e["pid"] for e in ends}, {codex.pid})
        self.assertEqual(self.leases(), {"alpha": alpha.pid})
        self.assertIn("not running", [ln for ln in self.do(alpha, "seats")["out"].splitlines() if SEAT in ln][0])

    def test_held_seat_is_per_seat(self):
        c1, c2 = self.engine(None, kind="codex"), self.engine(None, kind="codex")
        self.assertEqual(self.do(c1, "inbox", env=self.seat_env())["rc"], 0)
        r = self.do(c2, "inbox", env=self.seat_env())
        self.assertEqual(r["rc"], 1)
        self.assertIn(f"E_HELD: seat {SEAT} is held by pid {c1.pid}", r["err"])
        self.assertEqual(self.do(c2, "inbox", env=self.shared_env())["rc"], 0)   # `codex` is free for c2
        self.assertEqual(self.leases(), {SEAT: c1.pid, "codex": c2.pid})


class SpoolSeatRefusalTest(SpoolSeatBase):
    def assert_spool_refused(self, r, pid_kind):
        self.assertEqual(r["rc"], 1, r)
        self.assertIn("E_SEAT: this process has no seat: a request in seat codex-second-seat's spool", r["err"])

    def test_only_the_codex_app_server_gets_the_seat(self):
        named = self.engine(SEAT)                       # claude --remote-control codex-second-seat
        other = self.engine("alpha")
        nameless = self.engine(None)                    # claude -p
        helper = self.helper()                          # codex, not the listening app-server
        for stub in (named, other, nameless, helper):
            self.assert_spool_refused(self.do(stub, "inbox", env=self.seat_env()), None)
            r = self.do(stub, "hook", "session-start", env=self.seat_env())
            self.assertEqual(r["rc"], 0)                # the hook never fails, and shows no seat
        r = self.run_cli("send", "alpha", "--kind", "info", inp="x", env=self.seat_env())   # no engine at all
        self.assertEqual(r.returncode, 1)
        self.assertIn("E_SEAT: this process has no seat", r.stderr)
        for stub in (nameless, helper):
            r = self.do(stub, "register", SEAT, env=self.seat_env())
            self.assertEqual(r["rc"], 1)
            self.assertIn("E_SEAT", r["err"])
        rows = self.refused("spool_seat")
        self.assertTrue(rows)
        self.assertEqual({e["name"] for e in rows}, {SEAT})
        self.assertEqual({e["code"] for e in rows}, {"E_SEAT"})
        kinds = {(e["kind"], e["pid"]) for e in rows}
        self.assertIn(("claude", named.pid), kinds)
        self.assertIn(("claude", other.pid), kinds)
        self.assertIn(("codex", helper.pid), kinds)
        self.assertTrue(any(k is None for k, _ in kinds))
        self.assertNotIn(SEAT, self.leases())
        self.assertNotIn(SEAT, self.seat_rows())
        self.assertFalse([e for e in self.events("register") if e["name"] == SEAT])

    def test_no_other_way_to_the_seat(self):
        codex = self.engine(None, kind="codex")
        named = self.engine(SEAT)
        nameless = self.engine(None)
        # a Claude engine named for the seat: socket and shared spool
        for env in (self.env(), self.shared_env()):
            r = self.do(named, "inbox", env=env)
            self.assertEqual(r["rc"], 1, r)
            self.assertIn(f"E_SEAT: seat {SEAT} is reached only through its own spool", r["err"])
        # register: a nameless engine over the socket and the shared spool; Codex over the shared spool
        for env in (self.env(), self.shared_env()):
            r = self.do(nameless, "register", SEAT, env=env)
            self.assertEqual(r["rc"], 1, r)
            self.assertIn("reached only through its own spool", r["err"])
        r = self.do(codex, "register", SEAT, env=self.shared_env())
        self.assertIn("binding is automatic", r["out"])
        self.assertIn("you: codex", self.do(codex, "seats", env=self.shared_env())["out"])
        # request fields: a stamp field is refused as one; a seat or name field is an unknown field
        for env in (self.shared_env(), self.env(), self.seat_env()):
            for stub in (codex, named):
                rep = self.raw(stub, {"op": "inbox", "from": SEAT}, env)
                self.assertEqual(rep["code"], "E_FORMAT", rep)
                self.assertIn("stamp fields: from", rep["error"])
                for field in ("seat", "name", "spool"):
                    rep = self.raw(stub, {"op": "inbox", field: SEAT}, env)
                    self.assertEqual(rep["code"], "E_FORMAT", rep)
                    self.assertIn("unknown fields", rep["error"])
        self.assertTrue([e for e in self.events("refused") if e.get("what") == "stamp_field"])
        # AGENTMAIL_SPOOL naming the shared spool is the shared spool: Codex stays `codex`
        self.assertIn("you: codex", self.do(codex, "seats", env=self.seat_env(self.home / "spool"))["out"])
        r = self.do(codex, "seats", env=self.env(AGENTMAIL_SPOOL="relative/spool"))
        self.assertEqual(r["rc"], 2)
        self.assertNotIn(SEAT, self.leases())
        self.assertEqual({e["name"] for e in self.refused("spool_seat")}, {SEAT})
        # the real way still works afterwards
        self.assertIn(f"you: {SEAT}", self.do(codex, "seats", env=self.seat_env())["out"])

    def test_retired_seat_is_refused_like_any(self):
        self.reload(roster_with(spool_seat(SEAT, self.sp, "retired = true\n")))
        codex = self.engine(None, kind="codex")
        r = self.do(codex, "inbox", env=self.seat_env())
        self.assertEqual(r["rc"], 1)
        self.assertIn(f"E_SEAT: {SEAT} is retired", r["err"])


    def test_a_lease_taken_before_the_roster_named_the_spool_seat(self):
        """A Claude engine that took the name while it was an ordinary seat does not keep it."""
        self.reload(t2.ROSTER)
        named = self.engine(SEAT)
        self.assertIn(f"you: {SEAT}", self.do(named, "seats")["out"])
        self.reload(roster_with(spool_seat(SEAT, self.sp)))
        r = self.do(named, "inbox")
        self.assertEqual(r["rc"], 1)
        self.assertIn("reached only through its own spool", r["err"])
        codex = self.engine(None, kind="codex")
        self.assertIn(f"you: {SEAT}", self.do(codex, "seats", env=self.seat_env())["out"])
        self.assertEqual(self.leases()[SEAT], codex.pid)
        ends = [e for e in self.events("lease_end") if e["name"] == SEAT]
        self.assertEqual([(e["kind"], e["pid"]) for e in ends], [("claude", named.pid)])


class UnsafeSpoolTest(SpoolSeatBase):
    def roster_text(self):
        bad = self.root / "bad"
        for d in ("link-target", "groupw", "otherw"):
            (bad / d).mkdir(parents=True, mode=0o700)
        os.chmod(bad / "groupw", 0o770)
        os.chmod(bad / "otherw", 0o703)
        (bad / "link").symlink_to(bad / "link-target")
        (bad / "up").symlink_to(bad / "link-target")
        (bad / "link-target" / "spool").mkdir(mode=0o700)
        self.bad = {"s-link": bad / "link", "s-groupw": bad / "groupw", "s-otherw": bad / "otherw",
                    "s-missing": bad / "missing", "s-up": bad / "up" / "spool"}
        return roster_with(spool_seat(SEAT, self.sp), *(spool_seat(n, p) for n, p in self.bad.items()))

    def test_unsafe_spools_are_not_served(self):
        codex = self.engine(None, kind="codex")
        self.assertIn(f"you: {SEAT}", self.do(codex, "seats", env=self.seat_env())["out"])   # the good one
        time.sleep(1.0)                                                       # several spool passes
        rows = self.refused("roster")
        self.assertEqual(sorted(e["name"] for e in rows), sorted(self.bad))   # one line each
        for e in rows:
            self.assertEqual(e["code"], "E_FORMAT")
        self.assertFalse((self.bad["s-missing"]).exists())                    # never created
        self.assertFalse((self.bad["s-groupw"] / "req").exists())             # nothing created inside
        self.assertFalse((self.root / "bad" / "link-target" / "req").exists())
        planted = [self.plant(p) for n, p in self.bad.items() if n in ("s-groupw", "s-otherw")]
        time.sleep(0.8)
        for p, f in planted:
            self.assertTrue(p.exists(), p)                                    # never read
            self.assertFalse((p.parent.parent / "rep").exists())
            f.close()
            p.unlink()
        # a new roster load journals again, once
        self.reload(self.roster.read_text() + "\n[seat.zz-new]\n")
        self.assertEqual(len(self.refused("roster")), 2 * len(self.bad))
        time.sleep(0.6)
        self.assertEqual(len(self.refused("roster")), 2 * len(self.bad))
        # made safe: served without a restart (no roster change needed)
        os.chmod(self.bad["s-groupw"], 0o700)
        r = self.do(codex, "seats", env=self.seat_env(self.bad["s-groupw"]))
        self.assertIn("you: s-groupw", r["out"])
        # made unsafe while served: no longer served
        os.chmod(self.sp, 0o770)
        p, f = self.plant(self.sp)
        time.sleep(0.8)
        self.assertTrue(p.exists())
        f.close()


class WrongOwnerTest(SpoolSeatBase):
    server_kw = {"uid": os.getuid() + 1}            # the daemon expects another owner: ours is wrong

    def test_wrong_owner_is_not_served(self):
        self.assertTrue(self.wait_for(lambda: self.refused("roster")))
        time.sleep(0.6)
        rows = self.refused("roster")
        self.assertEqual([(e["name"], e["code"]) for e in rows], [(SEAT, "E_FORMAT")])
        self.assertIn("owned by uid", rows[0]["error"])
        self.assertFalse((self.sp / "req").exists())


class SpoolReloadTest(SpoolSeatBase):
    def roster_text(self):
        return t2.ROSTER

    def test_reload_adds_and_removes(self):
        codex = self.engine(None, kind="codex")
        p, f = self.plant(self.sp)
        time.sleep(0.6)
        self.assertTrue(p.exists())                                           # not a seat spool yet
        f.close()
        p.unlink()
        self.reload(roster_with(spool_seat(SEAT, self.sp)))
        self.assertIn(f"you: {SEAT}", self.do(codex, "seats", env=self.seat_env())["out"])
        self.reload(t2.ROSTER)
        p, f = self.plant(self.sp)
        time.sleep(0.8)
        self.assertTrue(p.exists())                                           # no longer served
        f.close()
        self.assertEqual(self.refused("roster"), [])


class SpoolReplyTest(SpoolSeatBase):
    def test_a_planted_link_is_not_followed(self):
        """A reply is never written through a link the spool's writer planted (rep/<rid>.tmp)."""
        codex = self.engine(None, kind="codex")
        self.assertEqual(self.do(codex, "inbox", env=self.seat_env())["rc"], 0)
        victim = self.root / "victim.txt"
        victim.write_text("keep")
        rid = uuid.uuid4().hex
        (self.sp / "rep" / f"{rid}.tmp").symlink_to(victim)
        code = ("import json, os, sys, time\nsp, rid = sys.argv[1], sys.argv[2]\n"
                "f = open(os.path.join(sp, 'req', rid + '.part'), 'wb')\nf.write(b'{\"op\": \"out\"}\\n')\nf.flush()\n"
                "os.rename(os.path.join(sp, 'req', rid + '.part'), os.path.join(sp, 'req', rid + '.json'))\n"
                "rep = os.path.join(sp, 'rep', rid + '.json')\n"
                "for _ in range(100):\n    time.sleep(0.1)\n    if os.path.exists(rep):\n"
                "        print(open(rep).read()); break\n")
        r = self.ctl(codex, argv=[sys.executable, "-I", "-c", code, str(self.sp), rid], env=self.env(), input="")
        self.assertTrue(json.loads(r["out"])["ok"], r)
        self.assertEqual(victim.read_text(), "keep")
        self.assertFalse((self.sp / "rep" / f"{rid}.tmp").exists())


if __name__ == "__main__":
    unittest.main()
