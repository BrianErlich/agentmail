"""The store cap (DESIGN.md §6.3): the send-side reserve and SQLite's hard page-count backstop.
Temporary directories only."""
import os
import unittest
from pathlib import Path
from unittest import mock

from tests import test_agentmail as t2

am, REPO = t2.am, t2.REPO


# ---- the store cap -------------------------------------------------------------

class StoreCapTest(t2.Base):
    def setUp(self):
        super().setUp()
        self.up(10, 30, 70)
        self.patches = []

    def tearDown(self):
        super().tearDown()
        for p in reversed(self.patches):
            p.stop()

    def patch(self, **values):
        for k, v in values.items():
            p = mock.patch.object(am, k, v)
            p.start()
            self.patches.append(p)
        self.restart()                    # the hard cap is set when the store is opened

    def size(self):
        return (self.home / "store.db").stat().st_size

    def count(self):
        return self.box.db.execute("SELECT count(*) FROM msg").fetchone()[0]

    def test_soft_limit_then_ack_and_close_still_work(self):
        req = self.ok(self.send(10, "webapp", kind="request", body="please"))["id"]
        info = self.ok(self.send(10, "webapp", body="for you"))["id"]
        page = self.box.db.execute("PRAGMA page_size").fetchone()[0]
        cap = self.size() + 64 * 1024 + 300 * 1024
        cap -= cap % page
        self.patch(STORE_CAP=cap, STORE_RESERVE=64 * 1024)
        body = "x" * (16 * 1024)
        sent, refused = 0, None
        for i in range(40):
            to = ("webapp", "carol-review")[i % 2]
            n = self.count()
            last = f"{i} {body}".encode()
            r = self.send(10, to, body=last, again=True)
            self.assertLessEqual(self.size(), cap)
            if r.get("ok"):
                sent += 1
                continue
            refused = r
            self.assertEqual(self.count(), n, "a refused send inserts nothing")
            break
        self.assertIsNotNone(refused, "the soft limit was never reached")
        self.code(refused, "E_FULL")
        self.assertEqual(refused["error"], "the store is at its 64 MiB cap")
        self.assertGreater(sent, 3)
        self.assertGreater(self.size() + 2 * len(last) + am.STORE_RESERVE, cap)
        self.ok(self.op(30, op="ack", msgs=[info]))                       # the reserve keeps these working
        self.ok(self.op(10, op="close", msg=req))
        self.assertEqual(self.row(req)["state"], "withdrawn")
        self.assertLessEqual(self.size(), cap)
        doc = self.ok(self.op(10, op="doctor"))
        self.assertTrue(am.store_ok(doc), doc)
        refusals = [e for e in self.events("refused") if e["code"] == "E_FULL"]
        self.assertEqual(refusals[-1]["what"], "other")

    def test_hard_backstop_returns_e_full_and_the_mailbox_keeps_answering(self):
        page = self.box.db.execute("PRAGMA page_size").fetchone()[0]
        cap = self.size() + 2 * page                                      # room for two more pages only
        self.patch(STORE_CAP=cap, STORE_RESERVE=-10 ** 9)                 # the soft check always passes
        n = self.count()
        r = self.code(self.send(10, "webapp", body="y" * am.MAX_BODY), "E_FULL")
        self.assertEqual(r["error"], "the store is at its 64 MiB cap")
        self.assertEqual(self.count(), n)
        self.assertFalse(self.box.db.in_transaction)
        self.assertLessEqual(self.size(), cap)
        self.ok(self.op(30, op="inbox"))                                  # the next call is answered
        self.ok(self.op(10, op="out"))
        doc = self.ok(self.op(10, op="doctor"))
        self.assertTrue(am.store_ok(doc), doc)
        self.assertTrue(any(e["code"] == "E_FULL" and e["what"] == "other" for e in self.events("refused")))

    def test_backstop_is_set_when_the_store_opens(self):
        page = self.box.db.execute("PRAGMA page_size").fetchone()[0]
        self.patch(STORE_CAP=4 * 1024 * 1024)
        self.assertEqual(self.box.db.execute("PRAGMA max_page_count").fetchone()[0], 4 * 1024 * 1024 // page)

    def test_doctor_store_check_at_the_boundary(self):
        self.assertTrue(am.store_ok({"store_bytes": am.STORE_CAP, "store_cap": am.STORE_CAP}))
        self.assertFalse(am.store_ok({"store_bytes": am.STORE_CAP + 1, "store_cap": am.STORE_CAP}))


if __name__ == "__main__":
    unittest.main()
