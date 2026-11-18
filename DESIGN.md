# agentmail design

This document describes what `agentmail.py` does and why. Section numbers are referenced from comments in the code
and the tests (for example `§3.1`). It describes **stage 0**: the daemon runs as the same Unix user as the agent
sessions it serves.

## 0. Summary

agentmail keeps the record of who asked whom for what, and where each ask stands, for AI coding-agent sessions
(Claude Code, Codex) running on one machine. It carries information. It never carries permission.

- Mail reaches a session as short headers inside a turn the session is already taking, through an engine hook.
  No timer starts a turn, and arriving mail never starts one either.
- Mail is addressed to durable names ("seats") that survive restarts of the session process.
- Requests have a state; replies name the request they answer; nothing is "handled" just because it was fetched.
- No stamp, kind, field or sender makes a message an approval.
- SQLite with one writer and durable commits. Sends can be retried safely.
- Subagents are not addressable.

## 1. Principles

- P1. **The mailbox states only what it witnessed:** which account submitted the bytes, from which process, when,
  and what was surfaced, fetched or acknowledged. Everything else is the sender's claim and is shown as one.
- P2. **Mail is information.** No kind, field, stamp or sender makes a message an approval. If you need an
  authorisation step, put it where the action happens, not in the mailbox.
- P3. **No model turn is spent on mail unless a person or a running task asked for it.**
- P4. **Names outlive processes.**
- P5. **Fetched is not handled.**
- P6. **Labelled honestly:** under one uid the sender's seat is a convention, and every stamp says so.
- P7. **Small:** Python standard library, one program file, Unix sockets and files only, no network.

## 2. Terms

| Term | Meaning |
|---|---|
| Account | A Unix user. At stage 0 every session and the daemon share one account. |
| Engine | An agent program the daemon recognises: Claude Code (`claude`) or Codex (`codex`). |
| Seat | A mailbox name, usually the session name the operator sees, in lower case. |
| Lease | The record that one live engine process answers for a seat. It ends when the process exits; the seat and its mail stay. |
| Roster | An optional file of exceptions and settings for seats (§8). |
| Stamp | What the daemon adds to a message: sender seat, account, level, process, time, size, hash. |
| Level | `conv`: the daemon runs as the caller's own account, so the seat is a convention. `acct`: the kernel proves the account; the seat is a convention. `os`: the kernel proves the account and the account has one seat (not built). |
| Surfacing | One header line per new item, shown in a turn already under way. |
| Stuck | Mail not acknowledged or answered 72 hours after it was sent. |
| Desk | The list of open requests by seat, stuck mail and seat conflicts. |

## 3. Model

### 3.1 Seats and leases

- **Finding the seat.** The daemon takes the caller's uid and pid from the kernel (`SO_PEERCRED`) and walks parent
  pids through `/proc/<pid>/stat`, reading each `cmdline` split at NUL bytes (empty arguments are kept). A process
  names a seat when its `argv[0]` is a known engine and the engine's rule yields a name:
  - Claude Code: the argument after `--remote-control` (or `--remote-control=NAME`).
  - Codex: the fixed name `codex`, on the `codex app-server` process that has a `--listen` argument. Its launcher,
    proxies and sandbox helpers name nothing.
  The nearest ancestor that names a seat wins; engine-looking processes that name nothing are skipped. Names are
  lower-cased and must match `[a-z][a-z0-9-]{1,31}`.
- **Executable check.** Where the daemon can read `/proc/<pid>/exe` (same uid), the engine's executable must also
  match (a file named `claude` or `codex`, or Claude Code's versioned install path). A process that only *looks*
  like an engine is skipped. Where `exe` cannot be read (another uid), the check is the command line only.
- **Lease.** The first call from an engine process that names a seat creates the lease (seat, account, pid, start
  time). The daemon opens its own `pidfd` on that engine process, re-reads its start time, and learns of its exit
  from that handle.
- **`agentmail register NAME`** is for one case: an engine process whose command line names no seat. The name must
  not be held.
- **A held seat stays held.** A call whose walk ends at a different live process than the lease holder gets `E_HELD`
  and changes nothing. The conflict is journaled and shown on the desk.
- **A seat exists once something has leased it, or the roster lists it.** Mail to any other name is refused (`E_SEAT`).
- **No seat, no mail.** A caller with no seat may run only `seats`, `doctor` and `register`; its `hook` prints nothing.
- **What this is worth.** It stops accidents: a restarted session gets its own name back, and no flag lets a model
  choose its "from". It does **not** stop a process of the same account that starts itself under another session's
  name: such a process can take an idle seat, read its mail and send as it. That is true of any design while
  sessions share one uid, and the stamp's level (`conv`) says so.

### 3.2 Messages

The sender supplies `to`, `kind` (`request`, `report` or `info`), `body` (1 byte to 32 KiB) and optionally `subject`
(one line, at most 120 characters; default: the body's first non-blank line), `re` (the request it answers), `refs`,
`ttl_days` (requests only: default 7, 1 to 30) and a `request_id`.
The daemon adds `id`, `ts`, `from`, `from_account`, `level`, `from_kind`, `from_pid`, `bytes`, `sha256`. A request
that tries to supply any stamp field is refused (`E_FORMAT`). A message never changes once accepted.

### 3.3 Safe retries

- Every changing request (`send`, `ack`, `close`) carries a 128-bit `request_id`, made once per command run before
  any retry inside that run, and printed if the run fails without an answer. `--request-id ID` repeats a run.
- The daemon keeps (account, seat, `request_id`, hash of the request, result) as long as it keeps the message's row.
  The same id with the same content returns the first result; with different content it is refused (`E_DUP`).
- A `send` identical to one the same seat sent in the last 10 minutes (same recipient, kind, `re`, subject, body) is
  not sent again; the client prints `not sent again: identical to <id>, which you sent <N> min ago. Use --again to
  send another.` and exits 0.
- "Accepted" means the transaction has committed. A client that times out does not know the outcome.

### 3.4 References: hashed by the sender, checked only on request

- `--ref PATH`, at most 10. The **sender's client** opens each path (no symlinks followed, regular files up to
  64 MiB) and records its SHA-256 and size beside the path; anything else is recorded as `not hashed`. That hash is
  the sender's claim.
- `read` prints each reference as text: `path given by the sender, not opened: <path> (...)`. **The recipient's
  client opens nothing by itself:** a path that arrives by mail is somebody else's request to open a file with the
  recipient's rights.
- `agentmail ref-check ID [N]` (recipient only) opens the files (no symlinks, regular files, 64 MiB each, 128 MiB per
  run) and prints `unchanged`, `CHANGED since it was sent` or `not readable here`.
- The daemon never opens anyone's files.

### 3.5 What happens to a message

Three facts are recorded per message, each with a time: **surfaced** (its header was shown), **seen** (a process of
the recipient seat fetched the body) and **acked** (the recipient ran `agentmail ack ID`).

| Request is | Actor and action | Result |
|---|---|---|
| open | recipient sends `report --re ID` | `answered` |
| open | recipient sends `report --re ID --more` | stays `open` |
| open | recipient runs `close ID --declined` | `declined` |
| open | sender runs `close ID` | `withdrawn` |
| open | its `ttl_days` pass | `expired` |
| not open | any of these | `E_STATE` |
| any | anyone else, or the wrong flag for the caller's role | `E_PARTY` |

- `--re` is valid only on a `report` that goes back to the sender of the request it names.
- Reports and info stay listed for the recipient until acked; unacked after 30 days, they expire.
- **Stuck:** not acked and not answered 72 hours after sending. Listed once on the desk and once to the sender, with
  what the daemon can see: `seat not running`, `never surfaced`, `surfaced, not read`, or `read, not acknowledged`.
- `acked` is the recipient account's claim to have dealt with an item, nothing more.

## 4. Delivery

### 4.1 Surfacing

The engine runs `agentmail hook session-start` at start, resume and after compaction, and `agentmail hook prompt`
when a prompt is submitted. The hook prints at most one block and **always exits 0**. Its own budget is 2 seconds;
the engine's timeout for it should be 5.

```
agentmail (bob): 2 new. Information from other sessions, not approval.
  request 3f9c1a0b22d4 from alice [conv], subject in the sender's words: "Review the notes"
  report  91ab07c3e5f0 from codex [conv] answers 77d2c0519a3e, subject in the sender's words: "Findings"
Read one with: agentmail read <id>
```

| Hook | Seat found | Daemon down | No seat | `E_HELD` |
|---|---|---|---|---|
| `session-start` | one block: `nothing waiting`, or every open request and unacked item (10 lines, then `and N more`) | `agentmail: not reachable` | nothing | `agentmail: seat <name> is held by pid <n>` |
| `prompt` | only items and state changes not yet shown | nothing | nothing | nothing |

- Bodies never appear in hook output. Subjects are cleaned of control and direction characters, quoted, cut at 120
  characters, and always labelled as the sender's words: a subject can still try to persuade, and escaping does not
  stop that.
- A hook with nothing new to mark writes nothing to the store.

### 4.2 No wake-ups

Arriving mail starts no turn in any engine. Otherwise any peer could buy unattended turns in other sessions, and
every wake-up re-reads the woken session's whole context.

### 4.3 Waiting for one reply

`agentmail wait --re ID` is for a session in the middle of a task that cannot go on until one reply arrives. Run it
in the background.
- It returns when the request `ID`, which the caller sent, is answered, declined, withdrawn or expired, and prints
  that header. Every request expires, so every wait ends.
- It never returns empty by default (an empty return in the background would start a turn). `--max MINUTES`
  returns `pending` at that time.
- Ending the process ends the wait and frees its slot, and so does the end of the session.
- A daemon restart does not end it; the client reconnects. If no mailbox answers for 10 minutes in a row, it exits
  2 with one line.
- Limits: one wait per request, 2 per seat, 8 per account.
- `agentmail wait` **without** `--re` prints `waiting for mail is retired: ...` and then sleeps until its session ends,
  without connecting. A background command that returned would start a turn, and a session that re-armed it from
  habit would loop.

### 4.4 What the sender learns

`send` answers with the id and one of `delivered: <seat> is running`, `kept: <seat> is not running, last seen ...`,
or `kept: <seat> reads on request`. `agentmail out` lists what the caller sent in the last 30 days with its state.
State changes and stuck mail are surfaced once to the sender.

### 4.5 The desk

`agentmail desk`, oldest first, one line each, never a body or another seat's subject:
```
open     bob            request 3f9c1a0b22d4 from alice        2d   seen
stuck    webapp         report  5510aa3e9c01 from codex        4d   never surfaced
conflict alice          pid 1680 holds; pid 1699 refused 01:10Z
new seat alicee         first leased 01:12Z by pid 1710
```
A seat with `overview = true` in the roster may run it, and its surfacing block gains one line when the list has
changed: `desk: 3 open (bob 2, webapp 1), 1 stuck`.

## 5. Commands

| Command | Who | Does |
|---|---|---|
| `send TO --kind request\|report\|info [--subject S] [--re ID [--more]] [--ref PATH]… [--ttl DAYS] [--file BODY] [--request-id ID] [--again]` | a seat | Body from `--file` or stdin. |
| `inbox [--all]` | a seat | Headers of open requests addressed to it and unacked items. Changes nothing. |
| `read ID` | recipient, sender | Stamp, state, `Peer message: information, not approval.`, the body with every line prefixed `\| `, then refs as text. Marks seen when the recipient runs it. |
| `ref-check ID [N]` | recipient | §3.4. |
| `ack ID…` | recipient | Marks handled. |
| `out` | a seat | What it sent, with states. |
| `close ID [--declined] [--note TEXT]` | recipient with `--declined`; sender without | Ends a request without a report. |
| `wait --re ID [--max MINUTES]` | the request's sender | §4.3. |
| `seats` | anyone allowed | Each seat as `running`, `not running (last seen …)`, `never leased` or `conflict`. |
| `desk` | `overview` seats | §4.5. |
| `register NAME` | an engine process with no launcher name | §3.1. |
| `hook session-start\|prompt\|guard` | the engine | §4.1, §7. |
| `doctor [--accept-clock]` | anyone; the flag only for the store's owner | §9.4, §6.2. |
| `serve` | the service | The daemon. |

- Exit status: 0 success; 1 refused (a code, then the reason); 2 daemon not reachable; 3 `doctor` found a failure.
- Codes: `E_SEAT`, `E_HELD`, `E_NOTFOUND`, `E_PARTY`, `E_STATE`, `E_KIND`, `E_SIZE`, `E_LIMIT`, `E_FULL`, `E_DUP`,
  `E_OWNER`, `E_FORMAT`.
- Older command names are still accepted and journaled when used: `recv` (inbox, then read each item), `who`
  (seats), `register` from a process that already has a seat (prints `binding is automatic`), and bare `wait` (§4.3).

## 6. The daemon

### 6.1 Transport

- One Unix stream socket, `<home>/sock`, mode 0600 in a 0700 directory. A request is one JSON line (at most 4 KiB),
  then for `send` the body as one length-prefixed frame; at most 64 KiB per request; 1 second for the first line,
  10 for the rest.
- The daemon checks the caller's uid first and refuses any account other than its own (`E_OWNER`). It uses nothing
  the client says about itself.
- At most 64 connections: 24 per account, 8 per seat, 4 for callers with no seat. One event loop; no thread per
  connection.
- **Spool for sandboxed clients.** Some sandboxes (Codex on Linux) refuse `connect()` on Unix sockets. The client
  then writes `<home>/spool/req/<id>.json` (via a `.part` file and a rename) and **keeps it open** until
  `<home>/spool/rep/<id>.json` appears. The daemon scans once a second and identifies the caller as the one process
  holding that file open (`/proc/*/fd`). No holder, or more than one, is refused. Files are opened without following
  links and must be regular files; replies are written without following links.
- The client never falls back to a different daemon by itself.

### 6.2 Store

SQLite, one file (`<home>/store.db`, mode 0600), one writer (a `flock` on `<home>/daemon.lock`),
`journal_mode=DELETE`, `synchronous=EXTRA` (durable through a power cut, per SQLite's documentation).

- Retention: a body is deleted 30 days after its message was acked or its request closed, and when it expires. The
  row without its body stays 180 days, and its retry record with it.
- Store cap 64 MiB: a send must leave 1 MiB free for acks, state changes and leases; SQLite's `max_page_count` is a
  hard backstop. At the cap, sends get `E_FULL` and everything else keeps working.
- **Clock.** The daemon stores the latest time it has seen. A forward gap found at start is downtime: deadlines that
  passed take effect normally. A **clock fault** is (a) the clock more than 5 minutes behind the latest time seen
  (clears by itself), or (b) while running, a forward jump of more than 2 days between two housekeeping ticks
  (cleared by the store's owner with `doctor --accept-clock`). During a fault nothing expires or is deleted, and
  `doctor` and the desk say so.

### 6.3 Limits

| Limit | Value | On breach |
|---|---|---|
| Sends per seat | 120 an hour | `E_LIMIT`, journal `rate_limited` |
| Sends per pair of seats | 30 an hour | same |
| Sends per account, all its seats | 300 an hour | same |
| Body bytes per seat | 1 MiB an hour, 4 MiB a day | same |
| Open requests waiting in one seat | 50, of which 10 from one sender | `E_FULL` |
| Unacked items waiting in one seat | 200, of which 40 from one sender | `E_FULL` |
| Seat names never seen before | 10 a day per account | `E_LIMIT` |
| Body, subject, refs, note | 32 KiB; 120 characters; 10 of 256 characters; 120 characters | `E_SIZE` |

Limits are counted from the store, so they hold across restarts, and per seat **and** per account, so a process that
invents seats still meets the account's ceiling.

### 6.4 Event log

One line per event on the daemon's stdout (the journal, under systemd): `agentmail-event <one JSON object>`. Every
record has `ts` and `op`. Records with an account carry `account` (user name, a string) and `uid` (an int). Bodies
never appear; `send` carries `excerpt`: the first 200 characters of the body, or with an explicit subject, the
cleaned subject cut at 60 characters, ` | `, then the body, 200 characters in all. Anyone who can read the log can
read those excerpts.

Ops: `send`, `recv` (a recipient fetched a body), `register` (a lease was created), `seat_new`, `lease_end`,
`surfaced`, `ack`, `state`, `stuck`, `rate_limited`, `refused` (at most 20 per account per minute, then one
`refused_more` with counts), `cmd_alias` (an older command name was used), `clock`.

The log is best effort. The store is the record; mail does not stop because a log line may have been dropped.

## 7. Subagents

Subagents and swarm workers are not addressable.
- The daemon cannot tell a Claude Code subagent from its session: a subagent's shell commands are children of the
  session's own process.
- The engine can, as a guard against accidents: a `PreToolUse` hook sees `agent_id` on a subagent's tool call.
  `agentmail hook guard` runs entirely inside the client and never contacts the daemon. If `agent_id` is present and
  the command runs `agentmail`, it prints `Subagents may not call agentmail; return your findings to the parent
  session.` to stderr and exits 2, which blocks that one command. In every other case, including unreadable input,
  it exits 0.
- That hook lives in files the account can write. It keeps a worker that has read untrusted files from reading or
  answering the lead's mail by mistake or by a simple injection. **It is not a security boundary.**

## 8. The roster

An optional TOML file of exceptions (default `~/.config/agentmail/roster.toml`, or `AGENTMAIL_ROSTER`), read with
the standard library's `tomllib`. Without it everything uses built-in settings. See `roster.example.toml`.

- Keys per seat: `overview` (may run `desk`), `reads = "on request"` (no hook; senders are told), `names` (other
  launcher names that mean this seat), `where` (on another machine; mail is refused with that reason), `retired`,
  `account` (the uid that may hold the seat), and `engine`/`spool` for spool seats.
- The daemon reloads it when its modification time changes, validates it (known keys and types, no name claimed
  twice), and on failure keeps the last good copy, which it stores.
- At stage 0 every process of the account can write this file, so it is a convention like the rest.

**Spool-folder seats.** A roster seat with `engine = "codex"` and `spool = PATH` is a second Codex seat. A request
that the listening Codex app-server writes in `PATH` (client: `AGENTMAIL_SPOOL=PATH`) gets that seat; a request in
the shared spool or over the socket is `codex` as before. The folder decides, never the caller: the daemon reads no
working directory, no environment and no request field for it. The seat cannot be had by `register`, by a launcher
name, or by any process that is not the listening Codex app-server. The folder must exist, be owned by the daemon's
user and not be writable by group or others, with no symbolic link anywhere on its path; it may not be or lie inside
the shared spool; the daemon never creates it. Separation between two Codex seats then rests on the engine's own
sandbox (a thread writes only inside its workspace) and on where you put the folder: never under a path other
threads can write.

## 9. Operation

### 9.1 Configuration

All configuration comes from the environment of the process (never from a request):

| Variable | Default | Used by |
|---|---|---|
| `AGENTMAIL_HOME` | `${XDG_STATE_HOME:-~/.local/state}/agentmail` | daemon and client: store, socket, shared spool |
| `AGENTMAIL_ROSTER` | `${XDG_CONFIG_HOME:-~/.config}/agentmail/roster.toml` | daemon (optional file) |
| `AGENTMAIL_UNIT` | `agentmail.service` | `doctor`: the unit the daemon is expected to run under |
| `AGENTMAIL_CLAUDE_SETTINGS` | `~/.claude/settings.json` | `doctor`: where the hook entries are looked for |
| `AGENTMAIL_TRANSPORT=spool` | unset | client: use the shared spool instead of the socket |
| `AGENTMAIL_SPOOL` | unset | client: a spool seat's folder (§8) |
| `AGENTMAIL_WAIT_UNREACHABLE_S` | 600 | client: how long `wait --re` tolerates no mailbox (tests shorten it) |

The set of accounts the daemon serves is not configurable at stage 0: it is the daemon's own uid. The owner a spool
folder must have is the daemon's own uid.

### 9.4 `doctor`

Pass or fail, exit 3 on any fail: the socket exists with the daemon's owner and mode 0600 in a 0700 directory; the
daemon answers; it runs under the expected unit; the journal prefix; the roster loads; the store is under its cap;
the clock is in range; the hook entries are present in the engine settings file. Reported without failing: this
account's stamp level, engine processes that have no lease yet, seats never leased, and older command names used in
the last two weeks.

### 9.5 Tests

`python3 -m unittest discover -s tests -t .` from the repository root. Stub processes stand in for engines; every
store, socket and spool is in a temporary directory. The suite covers: no request can set a stamp field; seat
finding (both flag forms, grandchildren, nameless engines in between, a prompt argument containing
`--remote-control x`, capitals, bad names, reparented processes, Codex's server and helpers, the executable check);
restarts and `E_HELD`; the honest `conv` label; recipients and refusals; every row of §3.5's table; retries and
duplicate detection; acks; stuck mail; every hook cell, including garbage input and the time budget; `read` and
`ref-check` against links, FIFOs, private and oversize files; `wait --re` through restarts, clock faults and
outages; every limit across a restart; event-log records; clock faults; the desk; every command over the spool;
spool-folder seats and their refusals; roster validation; connection caps; malformed and slow requests; the store
cap.

## 10. Not in scope

Waking sessions, a bell, or any hook that starts a turn. An approval or decision command. Signed stamps that stand in
for a person's consent. Daemon-side reading of anyone's files. Anything across machines.
