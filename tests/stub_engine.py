"""A stand-in engine process for agentmail's tests. The test chooses its argv (for example
`claude <this file> --remote-control name`), so /proc/<pid>/cmdline looks like an engine's.
It reads one JSON command per line on stdin and runs it as its own child:
  {"argv": [...], "input": "...", "env": {...}}   run and wait: {"rc", "out", "err"}
  {"argv": [...], "env": {...}, "bg": true}       start, do not wait: {"pid"}
  {"collect": pid, "timeout": s}                  wait for a started child: {"rc", "out", "err"} or {"timeout": true}
  {"kill": pid}                                   SIGKILL a started child
"""
import json
import subprocess
import sys

children = {}
for line in sys.stdin:
    cmd = json.loads(line)
    if "collect" in cmd:
        p = children[cmd["collect"]]
        try:
            out, err = p.communicate(timeout=cmd.get("timeout", 30))
            rep = {"rc": p.returncode, "out": out, "err": err}
        except subprocess.TimeoutExpired:
            rep = {"timeout": True}
    elif "kill" in cmd:
        children[cmd["kill"]].kill()
        children[cmd["kill"]].wait()
        rep = {"ok": True}
    elif cmd.get("bg"):
        p = subprocess.Popen(cmd["argv"], env=cmd.get("env"), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
        children[p.pid] = p
        rep = {"pid": p.pid}
    else:
        r = subprocess.run(cmd["argv"], input=cmd.get("input", ""), env=cmd.get("env"), capture_output=True,
                           text=True, timeout=cmd.get("timeout", 60))
        rep = {"rc": r.returncode, "out": r.stdout, "err": r.stderr}
    print(json.dumps(rep), flush=True)
