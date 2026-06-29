#!/usr/bin/env python3
"""
Check which Redis commands have been executed and whether each is supported by Dragonfly.
Compatibility data sourced from https://www.dragonflydb.io/docs/command-reference/compatibility
"""

import sys
import redis
import pandas as pd

# ---------------------------------------------------------------------------
# Dragonfly compatibility data (from dragonflydb.io/docs/command-reference/compatibility)
# ---------------------------------------------------------------------------

SUPPORTED = {
    # Bitmap
    "BITCOUNT", "BITFIELD", "BITFIELD_RO", "BITOP", "BITPOS", "GETBIT", "SETBIT",
    # Connection
    "AUTH", "CLIENT CACHING", "CLIENT GETNAME", "CLIENT ID", "CLIENT LIST",
    "CLIENT PAUSE", "CLIENT SETINFO", "CLIENT SETNAME", "CLIENT UNPAUSE", "CLIENT HELP",
    "ECHO", "HELLO", "PING", "QUIT", "SELECT",
    # Generic
    "DEL", "DELEX", "DUMP", "EXISTS", "EXPIRE", "EXPIREAT", "EXPIRETIME",
    "KEYS", "MOVE", "PERSIST", "PEXPIRE", "PEXPIREAT", "PEXPIRETIME", "PTTL",
    "RANDOMKEY", "RENAME", "RENAMENX", "RESTORE", "SCAN", "TOUCH", "TTL", "TYPE", "UNLINK",
    # Hash
    "HDEL", "HEXISTS", "HGET", "HGETALL", "HINCRBY", "HINCRBYFLOAT", "HKEYS", "HLEN",
    "HMGET", "HMSET", "HRANDFIELD", "HSCAN", "HSET", "HSETNX", "HSTRLEN", "HVALS", "HTTL",
    # HyperLogLog
    "PFADD", "PFMERGE", "PFCOUNT",
    # List
    "BLPOP", "BLMOVE", "BLMPOP", "BRPOP", "BRPOPLPUSH", "LINDEX", "LINSERT", "LLEN",
    "LMOVE", "LMPOP", "LPOP", "LPUSH", "LPUSHX", "LPOS", "LRANGE", "LREM", "LSET",
    "LTRIM", "RPOP", "RPOPLPUSH", "RPUSH", "RPUSHX",
    # PubSub
    "PSUBSCRIBE", "PUBLISH", "PUBSUB CHANNELS", "PUBSUB NUMPAT", "PUBSUB NUMSUB",
    "PUBSUB SHARDCHANNELS", "PUBSUB SHARDNUMSUB", "PUNSUBSCRIBE",
    "SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE", "SUBSCRIBE", "UNSUBSCRIBE",
    # Scripting
    "EVAL", "EVAL_RO", "EVALSHA", "EVALSHA_RO", "SCRIPT EXISTS", "SCRIPT LOAD",
    # Server
    "ACL CAT", "ACL DELUSER", "ACL GENPASS", "ACL GETUSER", "ACL LIST", "ACL LOG",
    "ACL USERS", "ACL WHOAMI", "COMMAND", "COMMAND COUNT", "COMMAND INFO",
    "CONFIG GET", "CONFIG RESETSTAT", "CONFIG SET", "DBSIZE", "FLUSHALL", "FLUSHDB",
    "INFO", "LASTSAVE", "MEMORY MALLOC-STATS", "MEMORY USAGE", "MONITOR",
    "REPLICAOF", "ROLE", "SAVE", "SHUTDOWN", "SLAVEOF",
    "SLOWLOG GET", "SLOWLOG HELP", "SLOWLOG LEN", "SLOWLOG RESET", "TIME",
    # Set
    "SADD", "SCARD", "SDIFF", "SDIFFSTORE", "SINTER", "SINTERCARD", "SINTERSTORE",
    "SISMEMBER", "SMEMBERS", "SMISMEMBER", "SMOVE", "SPOP", "SRANDMEMBER",
    "SREM", "SSCAN", "SUNION", "SUNIONSTORE",
    # Sorted Set
    "BZMPOP", "BZPOPMAX", "BZPOPMIN", "ZADD", "ZCARD", "ZCOUNT", "ZDIFF", "ZDIFFSTORE",
    "ZINCRBY", "ZINTER", "ZINTERCARD", "ZINTERSTORE", "ZLEXCOUNT", "ZMPOP", "ZMSCORE",
    "ZPOPMAX", "ZPOPMIN", "ZRANDMEMBER", "ZRANGE", "ZRANGEBYLEX", "ZRANGEBYSCORE",
    "ZRANGESTORE", "ZRANK", "ZREM", "ZREMRANGEBYLEX", "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE", "ZREVRANGE", "ZREVRANGEBYLEX", "ZREVRANGEBYSCORE",
    "ZREVRANK", "ZSCAN", "ZSCORE", "ZUNION", "ZUNIONSTORE",
    # Stream
    "XADD", "XACK", "XAUTOCLAIM", "XCLAIM", "XDEL", "XGROUP", "XINFO", "XLEN",
    "XPENDING", "XRANGE", "XREAD", "XREADGROUP", "XREVRANGE", "XSETID", "XTRIM",
    # String
    "APPEND", "DECR", "DECRBY", "GET", "GETDEL", "GETEX", "GETRANGE", "GETSET",
    "INCR", "INCRBY", "INCRBYFLOAT", "MGET", "MSET", "MSETNX", "PSETEX",
    "SET", "SETEX", "SETNX", "SETRANGE", "STRLEN", "SUBSTR",
    # Transactions
    "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH",
    # Geo
    "GEOADD", "GEODIST", "GEOHASH", "GEOPOS", "GEORADIUS", "GEORADIUS_RO",
    "GEORADIUSBYMEMBER", "GEORADIUSBYMEMBER_RO", "GEOSEARCH",
    # Bloom Filter
    "BF.ADD", "BF.MADD", "BF.EXISTS", "BF.MEXISTS",
    # Count-Min Sketch
    "CMS.INCRBY", "CMS.INFO", "CMS.INITBYDIM", "CMS.INITBYPROB", "CMS.MERGE", "CMS.QUERY",
    # JSON
    "JSON.ARRAPPEND", "JSON.ARRINDEX", "JSON.ARRINSERT", "JSON.ARRLEN", "JSON.ARRPOP",
    "JSON.ARRTRIM", "JSON.CLEAR", "JSON.DEBUG", "JSON.DEL", "JSON.FORGET", "JSON.GET",
    "JSON.MERGE", "JSON.MGET", "JSON.MSET", "JSON.NUMINCRBY", "JSON.NUMMULTBY",
    "JSON.OBJKEYS", "JSON.OBJLEN", "JSON.RESP", "JSON.SET", "JSON.STRAPPEND",
    "JSON.STRLEN", "JSON.TOGGLE", "JSON.TYPE",
    # Search (partial FT support)
    "FT._LIST", "FT.SYNUPDATE", "FT.SYNDUMP", "FT.CONFIG", "FT.HYBRID", "FT.ALTER", "FT.TAGVALS",
    # Top-K
    "TOPK.RESERVE", "TOPK.ADD", "TOPK.INCRBY", "TOPK.QUERY", "TOPK.COUNT", "TOPK.LIST", "TOPK.INFO",
    # Cluster
    "CLUSTER INFO", "CLUSTER NODES", "CLUSTER SHARDS", "CLUSTER SLOTS",
}

PARTIAL = {
    "COPY", "SORT", "SORT_RO", "HEXPIRE", "BF.RESERVE",
    "ACL DRYRUN", "ACL LOAD", "ACL SAVE", "ACL SETUSER",
    "BGSAVE", "CLIENT KILL", "CLIENT TRACKING", "MODULE LOAD",
    "FT.CREATE", "FT.SEARCH", "FT.DROPINDEX", "FT.INFO", "FT.PROFILE", "FT.AGGREGATE",
}

# ---------------------------------------------------------------------------
# Dragonfly cluster/swarm mode restrictions (sourced from Dragonfly GitHub
# src/server/cluster/cluster_family.cc and src/server/main_service.cc)
# ---------------------------------------------------------------------------

# Commands that return errors in cluster mode regardless of arguments.
# PUBLISH/SUBSCRIBE family: "not supported in cluster mode yet" (main_service.cc)
# SELECT: "SELECT is not allowed in cluster mode" (generic_family.cc) — DB 0 only
CLUSTER_DISABLED = {
    "PUBLISH", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE",
    "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE",
    "SELECT",
}

# Commands that work in cluster mode only when all keys hash to the same slot.
# Multi-key commands spanning different slots return: -CROSSSLOT Keys in request
# don't hash to the same slot. Use hash tags {tag} to co-locate related keys.
CLUSTER_CROSSSLOT = {
    # String
    "MGET", "MSET", "MSETNX",
    # Generic
    "DEL", "UNLINK", "RENAME", "RENAMENX", "COPY",
    # Set
    "SUNION", "SUNIONSTORE", "SINTER", "SINTERSTORE", "SDIFF", "SDIFFSTORE", "SMOVE",
    # Sorted set
    "ZUNIONSTORE", "ZINTERSTORE", "ZUNION", "ZINTER", "ZDIFF", "ZDIFFSTORE", "ZINTERCARD",
    # List
    "LMOVE", "BLMOVE", "BRPOPLPUSH", "RPOPLPUSH",
    # Bitmap
    "BITOP",
    # Scripting — scripts touching keys in multiple slots return CROSSSLOT
    "EVAL", "EVAL_RO", "EVALSHA", "EVALSHA_RO",
    # Transactions — all keys in MULTI/EXEC block and WATCH must share one slot
    "MULTI", "EXEC", "WATCH",
}


def cluster_classify(cmd: str) -> str:
    upper = cmd.upper()
    if upper in CLUSTER_DISABLED:
        return "Disabled"
    if upper in CLUSTER_CROSSSLOT:
        return "Cross-slot"
    return ""


UNSUPPORTED = {
    "CLIENT INFO", "CLIENT NO-EVICT", "CLIENT NO-TOUCH", "CLIENT REPLY",
    "CLIENT TRACKINGINFO", "CLIENT UNBLOCK",
    "MIGRATE", "OBJECT ENCODING", "OBJECT FREQ", "OBJECT IDLETIME", "OBJECT REFCOUNT",
    "WAIT", "WAITAOF", "PFDEBUG", "PFSELFTEST",
    "FCALL", "FUNCTION FLUSH", "FUNCTION DELETE", "FUNCTION DUMP", "FUNCTION LIST",
    "FUNCTION RESTORE", "FUNCTION STATS",
    "SCRIPT FLUSH", "SCRIPT DEBUG", "SCRIPT KILL",
    "MODULE LIST", "MODULE",
    "BGREWRITEAOF", "COMMAND DOCS", "COMMAND GETKEYS", "COMMAND GETKEYSANDFLAGS",
    "COMMAND LIST", "CONFIG REWRITE", "FAILOVER",
    "LATENCY DOCTOR", "LATENCY GRAPH", "LATENCY HISTOGRAM", "LATENCY HISTORY",
    "LATENCY LATEST", "LATENCY RESET",
    "LOLWUT", "MEMORY DOCTOR", "MEMORY PURGE", "MEMORY STATS",
    "MODULE LOADEX", "MODULE UNLOAD",
    "SWAPDB", "LCS", "GEOSEARCHSTORE",
    "BF.INSERT", "BF.SCANDUMP", "BF.LOADCHUNK", "BF.INFO", "BF.CARD", "BF.DEBUG",
    "JSON.DEBUG MEMORY",
    "READONLY", "READWRITE",
    "CLUSTER ADDSLOTS", "CLUSTER ADDSLOTSRANGE", "CLUSTER BUMPEPOCH",
    "CLUSTER COUNT-FAILURE-REPORTS", "CLUSTER COUNTKEYSINSLOT",
    "CLUSTER DELSLOTS", "CLUSTER DELSLOTRANGE", "CLUSTER FAILOVER",
    "CLUSTER FLUSHSLOTS", "CLUSTER FORGET", "CLUSTER GETKEYSINSLOT",
    "CLUSTER KEYSLOT", "CLUSTER LINKS", "CLUSTER MEET", "CLUSTER MYID",
    "CLUSTER MYSHARDID", "CLUSTER REPLICATE", "CLUSTER RESET",
    "CLUSTER SAVECONFIG", "CLUSTER SET-CONFIG-EPOCH", "CLUSTER SETSLOT",
    "CLUSTER SLAVES",
}


def classify(cmd: str) -> str:
    upper = cmd.upper()
    if upper in SUPPORTED:
        return "Supported"
    if upper in PARTIAL:
        return "Partial"
    if upper in UNSUPPORTED:
        return "Unsupported"
    # Subcommands reported as "XGROUP CREATE" — fall back to parent "XGROUP"
    parent = upper.split()[0]
    if parent != upper:
        if parent in SUPPORTED:
            return "Supported"
        if parent in PARTIAL:
            return "Partial"
        if parent in UNSUPPORTED:
            return "Unsupported"
    return "Unknown"


def build_dataframe(r: redis.Redis) -> pd.DataFrame:
    raw = r.info("commandstats")
    df = pd.DataFrame(raw).T
    df.index = df.index.str.replace("cmdstat_", "", regex=False)
    df.index.name = "Command"
    df = df.sort_values(by="calls", ascending=False)

    # Normalize index to uppercase for matching, keep display name as-is
    df["dragonfly_support"] = df.index.map(lambda c: classify(c.replace("|", " ")))
    df["cluster_mode"] = df.index.map(lambda c: cluster_classify(c.replace("|", " ")))
    return df


def main():
    host = "127.0.0.1"
    port = 7900
    password = None

    # Allow quick override via CLI: python check_dragonfly_compat.py host port [password]
    if len(sys.argv) >= 3:
        host = sys.argv[1]
        port = int(sys.argv[2])
    if len(sys.argv) >= 4:
        password = sys.argv[3]

    r = redis.Redis(host=host, port=port, password=password, decode_responses=True)

    try:
        r.ping()
    except redis.exceptions.ConnectionError as e:
        print(f"Cannot connect to Redis at {host}:{port} — {e}")
        sys.exit(1)

    df = build_dataframe(r)

    # Print summary table
    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 120)
    pd.set_option("display.max_colwidth", 40)

    display_cols = ["calls", "usec_per_call", "dragonfly_support", "cluster_mode"]
    available = [c for c in display_cols if c in df.columns]
    print(df[available].to_string())

    # Summary counts
    print("\n--- Summary ---")
    counts = df["dragonfly_support"].value_counts()
    for status, n in counts.items():
        print(f"  {status}: {n} command(s)")

    cluster_issues = df[df["cluster_mode"] != ""]
    if not cluster_issues.empty:
        print(f"\n  Cluster-mode restricted: {len(cluster_issues)} command(s)")

    unsupported = df[df["dragonfly_support"].isin(["Unsupported", "Partial"])]
    if not unsupported.empty:
        print("\n--- Commands needing attention ---")
        print(unsupported[available].to_string())

    if not cluster_issues.empty:
        print("\n--- Cluster mode restrictions ---")
        print("  Disabled   = command errors in cluster mode regardless of arguments")
        print("  Cross-slot = command errors if keys span different hash slots; use {hash-tag} to co-locate")
        print()
        print(cluster_issues[available].to_string())


if __name__ == "__main__":
    main()
