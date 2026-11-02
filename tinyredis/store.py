"""The data store and every command. It knows nothing about sockets, so it is easy to test."""
import bisect
import fnmatch
import random
import time


class CommandError(Exception):
    pass


WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"
NOT_INT = "ERR value is not an integer or out of range"
NOT_FLOAT = "ERR value is not a valid float"


class Simple(str):
    """A status reply such as +OK, as distinct from a bulk string."""


OK = Simple("OK")
WRITES = set("""SET SETNX GETSET DEL INCR DECR INCRBY DECRBY INCRBYFLOAT APPEND MSET EXPIRE PEXPIRE EXPIREAT PERSIST RENAME FLUSHALL FLUSHDB
LPUSH RPUSH LPOP RPOP LREM LSET LTRIM SADD SREM SPOP HSET HMSET HDEL HINCRBY ZADD ZREM ZINCRBY""".split())


class SortedSet:
    """A score map plus a sorted list of (score, member) for ordered queries."""

    def __init__(self):
        self.scores = {}
        self.items = []

    def add(self, member, score):
        old = self.scores.get(member)
        if old is not None:
            del self.items[bisect.bisect_left(self.items, (old, member))]
        self.scores[member] = score
        bisect.insort(self.items, (score, member))
        return old is None

    def remove(self, member):
        old = self.scores.pop(member, None)
        if old is None:
            return False
        del self.items[bisect.bisect_left(self.items, (old, member))]
        return True

    def rank(self, member):
        s = self.scores.get(member)
        return None if s is None else bisect.bisect_left(self.items, (s, member))

    def __len__(self):
        return len(self.items)


def fmt_float(x):
    return str(int(x)) if float(x).is_integer() and abs(x) < 1e17 else repr(float(x))


def to_int(s):
    try:
        return int(s)
    except (ValueError, TypeError):
        raise CommandError(NOT_INT) from None


def to_float(s):
    try:
        v = float(s)
    except (ValueError, TypeError):
        raise CommandError(NOT_FLOAT) from None
    if v != v:
        raise CommandError(NOT_FLOAT)
    return v


def slice_range(n, start, stop):
    if start < 0:
        start = max(n + start, 0)
    if stop < 0:
        stop = n + stop
    return start, min(stop, n - 1)


class Store:
    def __init__(self, clock=None, seed=None):
        self.data = {}
        self.expires = {}
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.rng = random.Random(seed)
        self.commands = {name[4:].upper(): getattr(self, name) for name in dir(self) if name.startswith("cmd_")}

    def execute(self, args):
        if not args:
            raise CommandError("ERR empty command")
        name = args[0].upper()
        fn = self.commands.get(name)
        if fn is None:
            raise CommandError(f"ERR unknown command '{args[0]}'")
        try:
            return fn(*args[1:])
        except TypeError:
            raise CommandError(f"ERR wrong number of arguments for '{name.lower()}' command") from None

    # keys

    def _alive(self, key):
        at = self.expires.get(key)
        if at is not None and at <= self.clock():
            del self.expires[key]
            self.data.pop(key, None)

    def _get(self, key, kind):
        self._alive(key)
        v = self.data.get(key)
        if v is not None and not isinstance(v, kind):
            raise CommandError(WRONGTYPE)
        return v

    def _make(self, key, kind):
        v = self._get(key, kind)
        if v is None:
            v = self.data[key] = kind()
        return v

    def _prune(self, key, v):
        if len(v) == 0:
            del self.data[key]
            self.expires.pop(key, None)

    def sweep(self, sample=20):
        """Remove a random sample of expired keys, so unread keys do not stay in memory."""
        keys = list(self.expires)
        for k in self.rng.sample(keys, min(sample, len(keys))):
            self._alive(k)

    def cmd_ping(self, msg=None):
        return Simple("PONG") if msg is None else msg

    def cmd_echo(self, msg):
        return msg

    def cmd_del(self, *keys):
        n = 0
        for k in keys:
            self._alive(k)
            if k in self.data:
                del self.data[k]
                self.expires.pop(k, None)
                n += 1
        return n

    def cmd_exists(self, *keys):
        n = 0
        for k in keys:
            self._alive(k)
            n += k in self.data
        return n

    def cmd_type(self, key):
        self._alive(key)
        v = self.data.get(key)
        names = {str: "string", list: "list", dict: "hash", set: "set", SortedSet: "zset"}
        return Simple(names.get(type(v), "none"))

    def cmd_keys(self, pattern):
        for k in list(self.data):
            self._alive(k)
        return sorted(k for k in self.data if fnmatch.fnmatchcase(k, pattern))

    def cmd_dbsize(self):
        for k in list(self.expires):
            self._alive(k)
        return len(self.data)

    def cmd_flushall(self):
        self.data.clear()
        self.expires.clear()
        return OK

    cmd_flushdb = cmd_flushall

    def cmd_rename(self, a, b):
        self._alive(a)
        if a not in self.data:
            raise CommandError("ERR no such key")
        self.data[b] = self.data.pop(a)
        self.expires.pop(b, None)
        if a in self.expires:
            self.expires[b] = self.expires.pop(a)
        return OK

    def cmd_expire(self, key, seconds):
        return self.cmd_pexpire(key, to_int(seconds) * 1000)

    def cmd_pexpire(self, key, ms):
        self._alive(key)
        if key not in self.data:
            return 0
        self.expires[key] = self.clock() + to_int(ms)
        self._alive(key)
        return 1

    def cmd_expireat(self, key, stamp_ms):
        self._alive(key)
        if key not in self.data:
            return 0
        self.expires[key] = to_int(stamp_ms)
        self._alive(key)
        return 1

    def cmd_pttl(self, key):
        self._alive(key)
        if key not in self.data:
            return -2
        at = self.expires.get(key)
        return -1 if at is None else at - self.clock()

    def cmd_ttl(self, key):
        v = self.cmd_pttl(key)
        return v if v < 0 else (v + 999) // 1000

    def cmd_persist(self, key):
        self._alive(key)
        return 1 if key in self.data and self.expires.pop(key, None) is not None else 0

    # strings

    def cmd_set(self, key, value, *opts):
        ex, nx, xx, get = None, False, False, False
        i = 0
        while i < len(opts):
            o = opts[i].upper()
            if o in ("EX", "PX"):
                i += 1
                if i >= len(opts):
                    raise CommandError("ERR syntax error")
                ex = to_int(opts[i]) * (1000 if o == "EX" else 1)
                if ex <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")
            elif o == "NX":
                nx = True
            elif o == "XX":
                xx = True
            elif o == "GET":
                get = True
            else:
                raise CommandError("ERR syntax error")
            i += 1
        self._alive(key)
        old = self.data.get(key)
        if get and old is not None and not isinstance(old, str):
            raise CommandError(WRONGTYPE)
        exists = key in self.data
        if (nx and exists) or (xx and not exists):
            return old if get else None
        self.data[key] = value
        self.expires.pop(key, None)
        if ex is not None:
            self.expires[key] = self.clock() + ex
        return old if get else OK

    def cmd_get(self, key):
        return self._get(key, str)

    def cmd_getset(self, key, value):
        old = self._get(key, str)
        self.data[key] = value
        self.expires.pop(key, None)
        return old

    def cmd_setnx(self, key, value):
        self._alive(key)
        if key in self.data:
            return 0
        self.data[key] = value
        return 1

    def cmd_mset(self, *pairs):
        if not pairs or len(pairs) % 2:
            raise CommandError("ERR wrong number of arguments for 'mset' command")
        for k, v in zip(pairs[::2], pairs[1::2]):
            self.data[k] = v
            self.expires.pop(k, None)
        return OK

    def cmd_mget(self, *keys):
        out = []
        for k in keys:
            self._alive(k)
            v = self.data.get(k)
            out.append(v if isinstance(v, str) else None)
        return out

    def cmd_incrby(self, key, by):
        v = self._get(key, str)
        n = to_int(v if v is not None else "0") + to_int(by)
        if not -(1 << 63) <= n < (1 << 63):
            raise CommandError("ERR increment or decrement would overflow")
        self.data[key] = str(n)
        return n

    def cmd_incr(self, key):
        return self.cmd_incrby(key, "1")

    def cmd_decr(self, key):
        return self.cmd_incrby(key, "-1")

    def cmd_decrby(self, key, by):
        return self.cmd_incrby(key, str(-to_int(by)))

    def cmd_incrbyfloat(self, key, by):
        v = self._get(key, str)
        n = to_float(v if v is not None else "0") + to_float(by)
        self.data[key] = fmt_float(n)
        return self.data[key]

    def cmd_append(self, key, value):
        v = self._get(key, str) or ""
        self.data[key] = v + value
        return len(self.data[key])

    def cmd_strlen(self, key):
        return len(self._get(key, str) or "")

    def cmd_getrange(self, key, start, stop):
        v = self._get(key, str) or ""
        a, b = slice_range(len(v), to_int(start), to_int(stop))
        return v[a:b + 1] if a <= b else ""

    # lists

    def cmd_lpush(self, key, *values):
        if not values:
            raise TypeError
        v = self._make(key, list)
        for x in values:
            v.insert(0, x)
        return len(v)

    def cmd_rpush(self, key, *values):
        if not values:
            raise TypeError
        v = self._make(key, list)
        v.extend(values)
        return len(v)

    def _pop(self, key, count, index):
        v = self._get(key, list)
        if v is None:
            return None
        if count is None:
            x = v.pop(index)
            self._prune(key, v)
            return x
        n = to_int(count)
        out = [v.pop(index) for _ in range(min(n, len(v)))]
        self._prune(key, v)
        return out

    def cmd_lpop(self, key, count=None):
        return self._pop(key, count, 0)

    def cmd_rpop(self, key, count=None):
        return self._pop(key, count, -1)

    def cmd_llen(self, key):
        return len(self._get(key, list) or [])

    def cmd_lrange(self, key, start, stop):
        v = self._get(key, list) or []
        a, b = slice_range(len(v), to_int(start), to_int(stop))
        return v[a:b + 1] if a <= b else []

    def cmd_lindex(self, key, i):
        v = self._get(key, list) or []
        i = to_int(i)
        return v[i] if -len(v) <= i < len(v) else None

    def cmd_lset(self, key, i, value):
        v = self._get(key, list)
        if v is None:
            raise CommandError("ERR no such key")
        i = to_int(i)
        if not -len(v) <= i < len(v):
            raise CommandError("ERR index out of range")
        v[i] = value
        return OK

    def cmd_lrem(self, key, count, value):
        v = self._get(key, list)
        if v is None:
            return 0
        count = to_int(count)
        limit = abs(count) or len(v)
        order = range(len(v) - 1, -1, -1) if count < 0 else range(len(v))
        hits = [i for i in order if v[i] == value][:limit]
        for i in sorted(hits, reverse=True):
            del v[i]
        self._prune(key, v)
        return len(hits)

    def cmd_ltrim(self, key, start, stop):
        v = self._get(key, list)
        if v is None:
            return OK
        a, b = slice_range(len(v), to_int(start), to_int(stop))
        v[:] = v[a:b + 1] if a <= b else []
        self._prune(key, v)
        return OK

    # sets

    def cmd_sadd(self, key, *members):
        if not members:
            raise TypeError
        v = self._make(key, set)
        before = len(v)
        v.update(members)
        return len(v) - before

    def cmd_srem(self, key, *members):
        v = self._get(key, set)
        if v is None:
            return 0
        n = sum(1 for m in members if m in v)
        v.difference_update(members)
        self._prune(key, v)
        return n

    def cmd_smembers(self, key):
        return sorted(self._get(key, set) or ())

    def cmd_sismember(self, key, m):
        return 1 if m in (self._get(key, set) or ()) else 0

    def cmd_scard(self, key):
        return len(self._get(key, set) or ())

    def cmd_spop(self, key):
        v = self._get(key, set)
        if not v:
            return None
        m = self.rng.choice(sorted(v))
        v.discard(m)
        self._prune(key, v)
        return m

    def _sets(self, keys):
        return [self._get(k, set) or set() for k in keys]

    def cmd_sinter(self, *keys):
        return sorted(set.intersection(*self._sets(keys)))

    def cmd_sunion(self, *keys):
        return sorted(set.union(*self._sets(keys)))

    def cmd_sdiff(self, *keys):
        s = self._sets(keys)
        return sorted(s[0].difference(*s[1:]))

    # hashes

    def cmd_hset(self, key, *pairs):
        if not pairs or len(pairs) % 2:
            raise CommandError("ERR wrong number of arguments for 'hset' command")
        v = self._make(key, dict)
        added = 0
        for f, x in zip(pairs[::2], pairs[1::2]):
            added += f not in v
            v[f] = x
        return added

    def cmd_hmset(self, key, *pairs):
        self.cmd_hset(key, *pairs)
        return OK

    def cmd_hget(self, key, f):
        return (self._get(key, dict) or {}).get(f)

    def cmd_hmget(self, key, *fields):
        v = self._get(key, dict) or {}
        return [v.get(f) for f in fields]

    def cmd_hdel(self, key, *fields):
        v = self._get(key, dict)
        if v is None:
            return 0
        n = 0
        for f in fields:
            if f in v:
                del v[f]
                n += 1
        self._prune(key, v)
        return n

    def cmd_hgetall(self, key):
        out = []
        for f, x in sorted((self._get(key, dict) or {}).items()):
            out += [f, x]
        return out

    def cmd_hkeys(self, key):
        return sorted(self._get(key, dict) or {})

    def cmd_hvals(self, key):
        v = self._get(key, dict) or {}
        return [v[f] for f in sorted(v)]

    def cmd_hlen(self, key):
        return len(self._get(key, dict) or {})

    def cmd_hexists(self, key, f):
        return 1 if f in (self._get(key, dict) or {}) else 0

    def cmd_hincrby(self, key, f, by):
        v = self._make(key, dict)
        n = to_int(v.get(f, "0")) + to_int(by)
        v[f] = str(n)
        return n

    # sorted sets

    def cmd_zadd(self, key, *pairs):
        if not pairs or len(pairs) % 2:
            raise CommandError("ERR syntax error")
        scores = [to_float(s) for s in pairs[::2]]
        v = self._make(key, SortedSet)
        return sum(v.add(m, s) for s, m in zip(scores, pairs[1::2]))

    def cmd_zscore(self, key, m):
        v = self._get(key, SortedSet)
        s = None if v is None else v.scores.get(m)
        return None if s is None else fmt_float(s)

    def cmd_zcard(self, key):
        v = self._get(key, SortedSet)
        return len(v) if v else 0

    def cmd_zrem(self, key, *members):
        v = self._get(key, SortedSet)
        if v is None:
            return 0
        n = sum(v.remove(m) for m in members)
        self._prune(key, v)
        return n

    def cmd_zincrby(self, key, by, m):
        v = self._make(key, SortedSet)
        s = v.scores.get(m, 0.0) + to_float(by)
        v.add(m, s)
        return fmt_float(s)

    def cmd_zrank(self, key, m):
        v = self._get(key, SortedSet)
        return None if v is None else v.rank(m)

    def _zout(self, items, opts):
        if any(o.upper() == "WITHSCORES" for o in opts):
            out = []
            for s, m in items:
                out += [m, fmt_float(s)]
            return out
        return [m for _, m in items]

    def cmd_zrange(self, key, start, stop, *opts):
        v = self._get(key, SortedSet)
        if v is None:
            return []
        a, b = slice_range(len(v), to_int(start), to_int(stop))
        return self._zout(v.items[a:b + 1] if a <= b else [], opts)

    def cmd_zrevrange(self, key, start, stop, *opts):
        v = self._get(key, SortedSet)
        if v is None:
            return []
        a, b = slice_range(len(v), to_int(start), to_int(stop))
        return self._zout(list(reversed(v.items))[a:b + 1] if a <= b else [], opts)

    def cmd_zrangebyscore(self, key, lo, hi, *opts):
        v = self._get(key, SortedSet)
        if v is None:
            return []

        def bound(s):
            s = s.lower()
            ex = s.startswith("(")
            t = s[1:] if ex else s
            return ex, float("inf") if t in ("+inf", "inf") else float("-inf") if t == "-inf" else to_float(t)
        lo_ex, lo_v = bound(lo)
        hi_ex, hi_v = bound(hi)
        items = [(s, m) for s, m in v.items if (s > lo_v if lo_ex else s >= lo_v) and (s < hi_v if hi_ex else s <= hi_v)]
        return self._zout(items, opts)

    def dump_commands(self):
        """The smallest list of commands that rebuilds the current data, for log rewriting."""
        now = self.clock()
        out = []
        for k in list(self.data):
            self._alive(k)
        for k, v in self.data.items():
            if isinstance(v, str):
                out.append(["SET", k, v])
            elif isinstance(v, list):
                out.append(["RPUSH", k, *v])
            elif isinstance(v, set):
                out.append(["SADD", k, *sorted(v)])
            elif isinstance(v, dict):
                c = ["HSET", k]
                for f, x in v.items():
                    c += [f, x]
                out.append(c)
            else:
                c = ["ZADD", k]
                for s, m in v.items:
                    c += [repr(s), m]
                out.append(c)
            if k in self.expires:
                out.append(["EXPIREAT", k, str(self.expires[k])])
        return out
