#!/usr/bin/env python3
"""
dragonfly_workload.py
─────────────────────
Generates a realistic, varied workload against a Dragonfly / Redis instance.

Each operation has a configured target rate (ops/sec).  The generator runs
all operations concurrently in threads, each sleeping to hit its target rate.
Operations are designed to be self-cleaning so memory stays bounded.

Default operations and rates
─────────────────────────────
  Name              Rate    Pattern
  ────────────────  ──────  ──────────────────────────────────────────────
  queue_push        300/s   LPUSH  L:{1..5}  <payload>
  queue_pop         295/s   RPOP   L:{1..5}            (drains the queue)
  counter_incr      100/s   INCRBY c:counter:{1..20}  <1..10>
  hash_write         50/s   HSET   h:record:{1..50}  field  value
  hash_read          80/s   HGET   h:record:{1..50}  field
  set_add            40/s   SADD   s:tags:{1..10}  tag:{1..20}
  sorted_add         60/s   ZADD   z:scores:{1..5}  <score>  member:{1..100}
  sorted_trim        55/s   ZREMRANGEBYRANK  z:scores:{1..5}  0  -101  (keep ≤100)
  json_write         30/s   JSON.SET  j:doc:{1..20}  $  <json>
  json_read          45/s   JSON.GET  j:doc:{1..20}
  key_expire         20/s   SET  e:tmp:{1..100}  val  EX 10
  string_get         70/s   GET  e:tmp:{1..100}
  pubsub_publish     10/s   SPUBLISH  ch:{1..5}  <message>    (shard pubsub)
  search_index       15/s   HSET  ft:item:{1..200}  + EXPIRE
  ft_search          25/s   FT.SEARCH  idx:items  <query>

Rates are approximate — each worker thread sleeps 1/rate seconds between ops
and adds a small jitter (±10%) so the load profile looks organic.

Usage
─────
  # Default 60s run against localhost:7900
  python dragonfly_workload.py

  # Custom duration and host
  python dragonfly_workload.py --host 127.0.0.1 --port 7900 --duration 120

  # Scale all rates by a factor (0.5 = half speed, 2.0 = double)
  python dragonfly_workload.py --rate-scale 0.5

  # Disable specific operations
  python dragonfly_workload.py --disable pubsub_publish ft_search

  # Show live stats every N seconds (default 5)
  python dragonfly_workload.py --stats-interval 10

  # Quiet mode (no per-op output, just final summary)
  python dragonfly_workload.py --quiet

  # Set a client name (CLIENT SETNAME) for each connection
  python dragonfly_workload.py --client-name workload-gen
 
  # use TLS with no CERT check:
  python df_workload_gen.py --use-tls true
"""

import argparse
import math
import random
import socket
import string
import sys
import threading
import time
import ssl
from collections import defaultdict
from datetime import datetime


# ──────────────────────────────────────────────────────────────────────────────
# Minimal RESP2 client
# ──────────────────────────────────────────────────────────────────────────────

class RawRedis:
    """Thread-safe-ish minimal RESP2 client (one connection per instance)."""

    def __init__(self, host, port, timeout=5, client_name="", use_tls=False, password=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.client_name = client_name
        self.use_tls = use_tls  
        self.password = password
        self._sock = None
        self._buf = b""
        self._lock = threading.Lock()

    def connect(self):
        # 1. Create the base TCP socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect((self.host, self.port))
        
        # 2. Upgrade the socket to TLS if requested
        if self.use_tls:
            # Create a default TLS context for client-side connections
            context = ssl.create_default_context()
            
            # Disable certificate verification and hostname checking
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            # Wrap the raw socket
            s = context.wrap_socket(s, server_hostname=self.host)
            
        self._sock = s
        self._buf = b""
        
        # 1. Authenticate FIRST before sending any other commands
        if self.password:
            # Pass three individual arguments: command, username, password
            self._send("AUTH", self.password)
            self._read_response()
        
        if self.client_name:
            self._send("CLIENT", "SETNAME", self.client_name)
            self._read_response()

    def close(self):
        if self._sock:
            try: self._sock.close()
            except Exception: pass
            self._sock = None

    def _encode(self, *args) -> bytes:
        """Correctly encodes arguments into a RESP Array packet."""
        # 1. Start with the number of elements in the array
        lines = [f"*{len(args)}".encode('utf-8')]
        
        # 2. Append each argument as a Bulk String: $<length>\r\n<value>
        for arg in args:
            # Convert to string (or bytes if it's already binary)
            if not isinstance(arg, bytes):
                arg_bytes = str(arg).encode('utf-8')
            else:
                arg_bytes = arg
                
            lines.append(f"${len(arg_bytes)}".encode('utf-8'))
            lines.append(arg_bytes)
            
        # 3. Join everything with RESP line endings (\r\n) and add a trailing one
        return b"\r\n".join(lines) + b"\r\n"

    def _send(self, *args):
        payload = self._encode(*args)
        #print(f"DEBUG SENDING: {payload!r}")
        self._sock.sendall(payload)

    def _readline(self):
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\r\n", 1)
        return line.decode()

    def _read_response(self):
        line = self._readline()
        t, body = line[0], line[1:]
        if t == "+": return body
        if t == "-": raise RuntimeError(body)
        if t == ":": return int(body)
        if t == "$":
            n = int(body)
            if n == -1: return None
            while len(self._buf) < n + 2:
                self._buf += self._sock.recv(4096)
            data, self._buf = self._buf[:n], self._buf[n + 2:]
            return data.decode(errors="replace")
        if t == "*":
            return [self._read_response() for _ in range(int(body))]
        raise ValueError(f"Unknown RESP byte: {t!r}")

    def command(self, *args):
        self._send(*args)
        return self._read_response()

    def ensure_ft_index(self):
        """Create ft idx:items if it doesn't exist (called once at startup)."""
        try:
            self.command(
                "FT.CREATE", "idx:items",
                "ON", "HASH",
                "PREFIX", "1", "ft:item:",
                "SCHEMA",
                "name",     "TEXT",
                "category", "TAG",
                "score",    "NUMERIC", "SORTABLE",
            )
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def rand_payload(size=16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))

def jitter(rate):
    """Return sleep interval for given rate with ±10% jitter."""
    base = 1.0 / rate
    return base * random.uniform(0.9, 1.1)

CATEGORIES = ["widgets", "gadgets", "tools", "parts", "supplies"]


# ──────────────────────────────────────────────────────────────────────────────
# Operation definitions
# ──────────────────────────────────────────────────────────────────────────────
#
# Each op is a function that accepts a RawRedis connection and returns the
# command name string (for stats).  The function may issue multiple commands
# (e.g. write + trim) — it should return the primary command name.
#

def op_queue_push(r):
    key = f"L:{random.randint(1, 5)}"
    r.command("LPUSH", key, rand_payload(32))
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "LPUSH"

def op_queue_pop(r):
    key = f"L:{random.randint(1, 5)}"
    r.command("RPOP", key)
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "RPOP"

def op_counter_incr(r):
    key = f"c:counter:{random.randint(1, 20)}"
    r.command("INCRBY", key, random.randint(1, 10))
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "INCRBY"

def op_hash_write(r):
    key   = f"h:record:{random.randint(1, 50)}"
    field = f"field{random.randint(1, 10)}"
    r.command("HSET", key, field, rand_payload(24))
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "HSET"

def op_hash_read(r):
    key   = f"h:record:{random.randint(1, 50)}"
    field = f"field{random.randint(1, 10)}"
    r.command("HGET", key, field)
    return "HGET"

def op_set_add(r):
    key = f"s:tags:{random.randint(1, 10)}"
    # Bounded: only 20 possible members per set
    r.command("SADD", key, f"tag:{random.randint(1, 20)}")
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "SADD"

def op_sorted_add(r):
    key    = f"z:scores:{random.randint(1, 5)}"
    score  = random.uniform(0, 1000)
    member = f"member:{random.randint(1, 100)}"
    r.command("ZADD", key, score, member)
    # Trim to keep at most 100 members — prevents unbounded growth
    r.command("ZREMRANGEBYRANK", key, 0, -101)
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "ZADD"

def op_json_write(r):
    key = f"j:doc:{random.randint(1, 20)}"
    doc = (
        f'{{"id":{random.randint(1,1000)},'
        f'"name":"{rand_payload(8)}",'
        f'"score":{random.uniform(0,100):.2f},'
        f'"active":{random.choice(["true","false"])}}}'
    )
    r.command("JSON.SET", key, "$", doc)
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "JSON.SET"

def op_json_read(r):
    key = f"j:doc:{random.randint(1, 20)}"
    r.command("JSON.GET", key)
    return "JSON.GET"

def op_key_expire(r):
    key = f"e:tmp:{random.randint(1, 100)}"
    r.command("SET", key, rand_payload(16), "EX", "10")
    return "SET"

def op_string_get(r):
    key = f"e:tmp:{random.randint(1, 100)}"
    r.command("GET", key)
    return "GET"

def op_pubsub_publish(r):
    channel = f"ch:{random.randint(1, 5)}"
    message = f"msg:{rand_payload(12)}"
    # SPUBLISH = shard pubsub (Dragonfly / Redis 7+)
    # Falls back to PUBLISH if SPUBLISH is unsupported
    try:
        r.command("SPUBLISH", channel, message)
        return "SPUBLISH"
    except RuntimeError:
        r.command("PUBLISH", channel, message)
        return "PUBLISH"

def op_search_index(r):
    idx  = random.randint(1, 200)
    key  = f"ft:item:{idx}"
    name = rand_payload(8)
    cat  = random.choice(CATEGORIES)
    score = random.uniform(0, 100)
    r.command("HSET", key, "name", name, "category", cat, "score", f"{score:.2f}")
    r.command("EXPIRE", key, 300)   # auto-expire after 5 min
    return "HSET(ft)"

def op_ft_search(r):
    queries = [
        ("*",                    []),
        ("@category:{widgets}",  []),
        ("@score:[50 100]",      []),
        ("@name:a*",             []),
        ("*",                    ["LIMIT", "0", "5"]),
        ("*",                    ["SORTBY", "score", "DESC", "LIMIT", "0", "10"]),
    ]
    q, extra = random.choice(queries)
    try:
        r.command("FT.SEARCH", "idx:items", q, *extra)
    except RuntimeError:
        pass   # index may not exist yet on first window
    return "FT.SEARCH"


# ──────────────────────────────────────────────────────────────────────────────
# Operation registry
# ──────────────────────────────────────────────────────────────────────────────

# (name, target_rate_per_sec, function)
OPERATIONS = [
    ("queue_push",      300, op_queue_push),
    ("queue_pop",       295, op_queue_pop),
    ("counter_incr",    100, op_counter_incr),
    ("hash_read",        80, op_hash_read),
    ("string_get",       70, op_string_get),
    ("sorted_add",       60, op_sorted_add),
    ("hash_write",       50, op_hash_write),
    ("set_add",          40, op_set_add),
    ("json_read",        45, op_json_read),
    ("ft_search",        25, op_ft_search),
    ("key_expire",       20, op_key_expire),
    ("search_index",     15, op_search_index),
    ("json_write",       30, op_json_write),
    ("pubsub_publish",   10, op_pubsub_publish),
]


# ──────────────────────────────────────────────────────────────────────────────
# Worker thread
# ──────────────────────────────────────────────────────────────────────────────

class Worker(threading.Thread):
    def __init__(self, name, rate, func, host, port, stop_event,
                 counters, counter_lock, client_name, rate_scale, quiet, use_tls, password=None):
        super().__init__(name=name, daemon=True)
        self.rate         = rate * rate_scale
        self.func         = func
        self.host         = host
        self.port         = port
        self.stop_event   = stop_event
        self.counters     = counters
        self.counter_lock = counter_lock
        self.client_name  = client_name
        self.quiet        = quiet
        self.ops_done     = 0
        self.errors       = 0
        self.use_tls      = use_tls
        self.password     = password

    def run(self):
        r = RawRedis(self.host, self.port, client_name=self.client_name, use_tls=self.use_tls, password=self.password)
        try:
            r.connect()
        except Exception as e:
            print(f"[{self.name}] connect failed: {e}", file=sys.stderr)
            return

        while not self.stop_event.is_set():
            t0 = time.monotonic()
            try:
                cmd = self.func(r)
                self.ops_done += 1
                with self.counter_lock:
                    self.counters[cmd] += 1
            except Exception as e:
                self.errors += 1
                if not self.quiet:
                    print(f"  [{self.name}] ERR: {e}", file=sys.stderr)

            elapsed = time.monotonic() - t0
            sleep   = max(0, jitter(self.rate) - elapsed)
            self.stop_event.wait(sleep)

        r.close()


# ──────────────────────────────────────────────────────────────────────────────
# Stats printer
# ──────────────────────────────────────────────────────────────────────────────

def stats_printer(workers, counters, counter_lock, stop_event, interval, start_time):
    prev_counts = defaultdict(int)
    prev_time   = time.monotonic()

    while not stop_event.wait(interval):
        now      = time.monotonic()
        elapsed  = now - prev_time
        run_time = now - start_time

        with counter_lock:
            snapshot = dict(counters)

        deltas     = {k: snapshot.get(k, 0) - prev_counts.get(k, 0) for k in snapshot}
        total_ops  = sum(snapshot.values())
        delta_ops  = sum(deltas.values())
        rate_now   = delta_ops / elapsed if elapsed > 0 else 0
        total_rate = total_ops / run_time if run_time > 0 else 0

        print(f"\n── [{datetime.now():%H:%M:%S}]  +{elapsed:.1f}s  "
              f"rate={rate_now:.0f}/s  total={total_ops:,}  avg={total_rate:.0f}/s ──")

        # Sort by current-window delta
        for cmd, delta in sorted(deltas.items(), key=lambda x: -x[1]):
            if delta > 0:
                r = delta / elapsed
                print(f"  {cmd:<20} {delta:>6} ops  ({r:>6.1f}/s)")

        total_errors = sum(w.errors for w in workers)
        if total_errors:
            print(f"  errors: {total_errors}")

        prev_counts = snapshot
        prev_time   = now


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Dragonfly / Redis workload generator")
    p.add_argument("--host",           default="127.0.0.1",
                   help="Target host (default 127.0.0.1)")
    p.add_argument("--port",           type=int, default=7900,
                   help="Target port (default 7900)")
    p.add_argument("--duration",       type=int, default=60,
                   help="Run duration in seconds (default 60)")
    p.add_argument("--rate-scale",     type=float, default=1.0,
                   help="Multiply all rates by this factor (default 1.0)")
    p.add_argument("--disable",        nargs="*", default=[],
                   metavar="OP",
                   help="Operation names to disable (space-separated)")
    p.add_argument("--stats-interval", type=int, default=5,
                   help="Print live stats every N seconds (default 5)")
    p.add_argument("--client-name",    default="workload-gen",
                   help="CLIENT SETNAME value for all connections (default: workload-gen)")
    p.add_argument("--quiet",          action="store_true",
                   help="Suppress per-error output (final summary still shown)")
    p.add_argument("--list-ops",       action="store_true",
                   help="Print all operation names and default rates, then exit")
    p.add_argument("--password", type=str,
                   help="Authenticate with password (uses default user)")
    p.add_argument("--use_tls",       action="store_true",
                   help="Connect using SSL (no CERT check)")
    # Unpack the tuple into 'args' and a throwaway variable '_'
    args, _   = p.parse_known_args()

    if args.list_ops:
        print(f"{'Name':<22} {'Rate (ops/s)':>12}")
        print(f"{'-'*22} {'-'*12}")
        for name, rate, _ in OPERATIONS:
            print(f"  {name:<20} {rate:>12}")
        return

    disabled = set(args.disable or [])
    ops      = [(n, r, f) for n, r, f in OPERATIONS if n not in disabled]

    if not ops:
        sys.exit("All operations disabled — nothing to run.")

    # ── startup: connect and create FT index ─────────────────────────────
    print(f"[{datetime.now():%H:%M:%S}] Connecting to {args.host}:{args.port} …")
    try:
        setup = RawRedis(args.host, args.port, client_name=args.client_name + "-setup", use_tls=args.use_tls, password=args.password )
        setup.connect()
        if not any(n == "ft_search" for n, _, _ in ops) is False:
            try:
                setup.ensure_ft_index()
                print("  [setup] idx:items ready")
            except Exception as e:
                print(f"  [setup] FT index warning: {e}")
        setup.close()
    except Exception as e:
        sys.exit(f"Cannot connect to {args.host}:{args.port} – {e}")

    # ── launch workers ────────────────────────────────────────────────────
    stop_event   = threading.Event()
    counters     = defaultdict(int)
    counter_lock = threading.Lock()
    workers      = []

    print(f"\n[{datetime.now():%H:%M:%S}] Starting {len(ops)} operation workers "
          f"(rate_scale={args.rate_scale}x, duration={args.duration}s)\n")
    print(f"  {'Operation':<22} {'Target rate':>12}")
    print(f"  {'-'*22} {'-'*12}")
    for name, rate, func in ops:
        effective = rate * args.rate_scale
        print(f"  {name:<22} {effective:>10.1f}/s")
        w = Worker(
            name=name, rate=rate, func=func,
            host=args.host, port=args.port,
            stop_event=stop_event,
            counters=counters, counter_lock=counter_lock,
            client_name=args.client_name,
            rate_scale=args.rate_scale,
            quiet=args.quiet,
            use_tls=args.use_tls,
            password=args.password,
        )
        workers.append(w)
        w.start()

    # ── stats printer thread ──────────────────────────────────────────────
    start_time = time.monotonic()
    st = threading.Thread(
        target=stats_printer,
        args=(workers, counters, counter_lock, stop_event,
              args.stats_interval, start_time),
        daemon=True,
    )
    st.start()

    # ── run for duration ──────────────────────────────────────────────────
    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\n[interrupted]")

    stop_event.set()

    print(f"\n[{datetime.now():%H:%M:%S}] Stopping workers …")
    for w in workers:
        w.join(timeout=3)

    # ── final summary ─────────────────────────────────────────────────────
    run_secs    = time.monotonic() - start_time
    total_ops   = sum(counters.values())
    total_errs  = sum(w.errors for w in workers)
    avg_rate    = total_ops / run_secs if run_secs > 0 else 0

    print(f"\n{'='*55}")
    print(f"WORKLOAD SUMMARY  ({run_secs:.1f}s)")
    print(f"{'='*55}")
    print(f"  Total ops  : {total_ops:>10,}")
    print(f"  Avg rate   : {avg_rate:>10.1f} ops/s")
    print(f"  Errors     : {total_errs:>10,}")
    print(f"\n  {'Command':<22} {'Ops':>10} {'Rate/s':>10}")
    print(f"  {'-'*22} {'-'*10} {'-'*10}")
    for cmd, cnt in sorted(counters.items(), key=lambda x: -x[1]):
        print(f"  {cmd:<22} {cnt:>10,} {cnt/run_secs:>10.1f}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
