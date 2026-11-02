import asyncio
import os
import random
import shutil
import tempfile
import unittest

from tinyredis import Client, CommandError, Server, SortedSet, Store, encode, parse_frame, replay


class Clock:
    def __init__(self):
        self.now = 1_000_000

    def __call__(self):
        return self.now


class Base(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.s = Store(clock=self.clock, seed=1)

    def c(self, *args):
        return self.s.execute([str(a) for a in args])


class TestStrings(Base):
    def test_set_get(self):
        self.assertEqual(self.c("SET", "a", "1"), "OK")
        self.assertEqual(self.c("get", "a"), "1")
        self.assertIsNone(self.c("GET", "missing"))

    def test_set_options(self):
        self.assertIsNone(self.c("SET", "a", "1", "XX"))
        self.assertEqual(self.c("SET", "a", "1", "NX"), "OK")
        self.assertIsNone(self.c("SET", "a", "2", "NX"))
        self.assertEqual(self.c("SET", "a", "3", "GET"), "1")
        with self.assertRaises(CommandError):
            self.c("SET", "a", "1", "BOGUS")
        with self.assertRaises(CommandError):
            self.c("SET", "a", "1", "EX", "0")

    def test_counters(self):
        self.assertEqual(self.c("INCR", "n"), 1)
        self.assertEqual(self.c("INCRBY", "n", 9), 10)
        self.assertEqual(self.c("DECRBY", "n", 4), 6)
        self.assertEqual(self.c("INCRBYFLOAT", "n", "1.5"), "7.5")
        self.c("SET", "t", "abc")
        with self.assertRaises(CommandError):
            self.c("INCR", "t")
        self.c("SET", "big", 2 ** 63 - 1)
        with self.assertRaises(CommandError):
            self.c("INCR", "big")

    def test_multi_key(self):
        self.c("MSET", "a", 1, "b", 2)
        self.c("LPUSH", "l", 1)
        self.assertEqual(self.c("MGET", "a", "x", "b", "l"), ["1", None, "2", None])
        self.assertEqual(self.c("APPEND", "a", "z"), 2)
        self.assertEqual(self.c("STRLEN", "a"), 2)
        self.assertEqual(self.c("GETRANGE", "a", 0, -1), "1z")


class TestExpiry(Base):
    def test_ttl_flow(self):
        self.c("SET", "a", 1, "EX", 10)
        self.assertEqual(self.c("TTL", "a"), 10)
        self.clock.now += 9500
        self.assertEqual(self.c("TTL", "a"), 1)
        self.assertEqual(self.c("PTTL", "a"), 500)
        self.clock.now += 500
        self.assertIsNone(self.c("GET", "a"))
        self.assertEqual(self.c("TTL", "a"), -2)

    def test_no_expiry_and_persist(self):
        self.c("SET", "a", 1)
        self.assertEqual(self.c("TTL", "a"), -1)
        self.assertEqual(self.c("EXPIRE", "a", 5), 1)
        self.assertEqual(self.c("PERSIST", "a"), 1)
        self.clock.now += 99999
        self.assertEqual(self.c("GET", "a"), "1")

    def test_set_clears_expiry(self):
        self.c("SET", "a", 1, "PX", 100)
        self.c("SET", "a", 2)
        self.clock.now += 1000
        self.assertEqual(self.c("GET", "a"), "2")

    def test_expired_keys_do_not_count(self):
        self.c("SET", "a", 1, "PX", 10)
        self.c("SET", "b", 1)
        self.clock.now += 11
        self.assertEqual(self.c("DBSIZE"), 1)
        self.assertEqual(self.c("KEYS", "*"), ["b"])
        self.assertEqual(self.c("EXISTS", "a", "b"), 1)

    def test_sweep_frees_unread_keys(self):
        for i in range(10):
            self.c("SET", f"k{i}", 1, "PX", 5)
        self.clock.now += 10
        self.s.sweep(100)
        self.assertEqual(len(self.s.data), 0)


class TestCollections(Base):
    def test_lists(self):
        self.assertEqual(self.c("RPUSH", "l", 1, 2, 3), 3)
        self.assertEqual(self.c("LPUSH", "l", 0), 4)
        self.assertEqual(self.c("LRANGE", "l", 0, -1), ["0", "1", "2", "3"])
        self.assertEqual(self.c("LRANGE", "l", -2, -1), ["2", "3"])
        self.assertEqual(self.c("LINDEX", "l", -1), "3")
        self.assertEqual(self.c("LPOP", "l"), "0")
        self.assertEqual(self.c("RPOP", "l", 2), ["3", "2"])
        self.assertEqual(self.c("LPOP", "l"), "1")
        self.assertEqual(self.c("EXISTS", "l"), 0)

    def test_list_edit(self):
        self.c("RPUSH", "l", "a", "b", "a", "c", "a")
        self.assertEqual(self.c("LREM", "l", 2, "a"), 2)
        self.assertEqual(self.c("LRANGE", "l", 0, -1), ["b", "c", "a"])
        self.assertEqual(self.c("LREM", "l", -1, "a"), 1)
        self.c("LSET", "l", 0, "z")
        self.c("LTRIM", "l", 0, 0)
        self.assertEqual(self.c("LRANGE", "l", 0, -1), ["z"])

    def test_sets(self):
        self.assertEqual(self.c("SADD", "s", "a", "b", "a"), 2)
        self.c("SADD", "t", "b", "c")
        self.assertEqual(self.c("SINTER", "s", "t"), ["b"])
        self.assertEqual(self.c("SUNION", "s", "t"), ["a", "b", "c"])
        self.assertEqual(self.c("SDIFF", "s", "t"), ["a"])
        self.assertEqual(self.c("SISMEMBER", "s", "a"), 1)
        self.assertEqual(self.c("SREM", "s", "a", "zz"), 1)
        self.assertEqual(self.c("SCARD", "s"), 1)

    def test_hashes(self):
        self.assertEqual(self.c("HSET", "h", "a", 1, "b", 2), 2)
        self.assertEqual(self.c("HSET", "h", "a", 9), 0)
        self.assertEqual(self.c("HGET", "h", "a"), "9")
        self.assertEqual(self.c("HGETALL", "h"), ["a", "9", "b", "2"])
        self.assertEqual(self.c("HINCRBY", "h", "b", 5), 7)
        self.assertEqual(self.c("HMGET", "h", "b", "no"), ["7", None])
        self.assertEqual(self.c("HDEL", "h", "a", "b"), 2)
        self.assertEqual(self.c("TYPE", "h"), "none")

    def test_sorted_sets(self):
        self.assertEqual(self.c("ZADD", "z", 3, "c", 1, "a", 2, "b"), 3)
        self.assertEqual(self.c("ZRANGE", "z", 0, -1), ["a", "b", "c"])
        self.assertEqual(self.c("ZREVRANGE", "z", 0, 0, "WITHSCORES"), ["c", "3"])
        self.assertEqual(self.c("ZRANK", "z", "b"), 1)
        self.assertEqual(self.c("ZINCRBY", "z", 10, "a"), "11")
        self.assertEqual(self.c("ZRANGE", "z", 0, -1), ["b", "c", "a"])
        self.assertEqual(self.c("ZRANGEBYSCORE", "z", "(2", "+inf"), ["c", "a"])
        self.assertEqual(self.c("ZSCORE", "z", "b"), "2")
        self.assertEqual(self.c("ZREM", "z", "b"), 1)
        self.assertEqual(self.c("ZCARD", "z"), 2)

    def test_sorted_set_ties_sort_by_member(self):
        self.c("ZADD", "z", 1, "b", 1, "a")
        self.assertEqual(self.c("ZRANGE", "z", 0, -1), ["a", "b"])

    def test_sorted_set_against_a_plain_model(self):
        rng = random.Random(1)
        z, model = SortedSet(), {}
        for _ in range(3000):
            m = f"m{rng.randint(0, 50)}"
            if rng.random() < 0.3:
                self.assertEqual(z.remove(m), m in model)
                model.pop(m, None)
            else:
                s = rng.randint(0, 20)
                z.add(m, s)
                model[m] = s
            self.assertEqual(z.items, sorted((s, m) for m, s in model.items()))

    def test_wrong_type(self):
        self.c("SET", "a", 1)
        for cmd in (["LPUSH", "a", 1], ["HGET", "a", "f"], ["SADD", "a", 1], ["ZADD", "a", 1, "m"]):
            with self.assertRaises(CommandError) as c:
                self.c(*cmd)
            self.assertIn("WRONGTYPE", str(c.exception))

    def test_bad_usage(self):
        with self.assertRaises(CommandError):
            self.c("NOSUCH")
        with self.assertRaises(CommandError):
            self.c("GET")
        with self.assertRaises(CommandError):
            self.c("GET", "a", "b")


class TestProtocol(unittest.TestCase):
    def test_encode(self):
        self.assertEqual(encode("hi"), b"$2\r\nhi\r\n")
        self.assertEqual(encode(5), b":5\r\n")
        self.assertEqual(encode(None), b"$-1\r\n")
        self.assertEqual(encode([1, "a"]), b"*2\r\n:1\r\n$1\r\na\r\n")

    def test_parse_handles_partial_input(self):
        full = b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"
        for i in range(len(full)):
            self.assertEqual(parse_frame(full[:i]), (None, 0))
        self.assertEqual(parse_frame(full), (["GET", "a"], len(full)))

    def test_inline_commands(self):
        self.assertEqual(parse_frame(b"PING hello\r\n"), (["PING", "hello"], 12))


class TestServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dir = tempfile.mkdtemp()
        self.aof = os.path.join(self.dir, "a.aof")
        self.server = await Server(aof=self.aof, port=0).start()
        self.c = await Client.connect("127.0.0.1", self.server.port)

    async def asyncTearDown(self):
        await self.c.close()
        await self.server.stop()
        shutil.rmtree(self.dir, ignore_errors=True)

    async def test_round_trip(self):
        self.assertEqual(await self.c.call("PING"), "PONG")
        self.assertEqual(await self.c.call("SET", "k", "héllo"), "OK")
        self.assertEqual(await self.c.call("GET", "k"), "héllo")
        self.assertEqual(await self.c.call("RPUSH", "l", 1, 2), 2)
        self.assertEqual(await self.c.call("LRANGE", "l", 0, -1), ["1", "2"])
        with self.assertRaises(CommandError):
            await self.c.call("NOSUCH")

    async def test_pipeline_in_one_write(self):
        self.c.w.write(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
        await self.c.w.drain()
        self.assertEqual([await self.c.reply(), await self.c.reply()], ["PONG", "PONG"])

    async def test_split_across_writes(self):
        self.c.w.write(b"*2\r\n$3\r\nGE")
        await self.c.w.drain()
        await asyncio.sleep(0.02)
        self.c.w.write(b"T\r\n$1\r\nz\r\n")
        await self.c.w.drain()
        self.assertIsNone(await self.c.reply())

    async def test_transaction(self):
        self.assertEqual(await self.c.call("MULTI"), "OK")
        self.assertEqual(await self.c.call("INCR", "n"), "QUEUED")
        self.assertEqual(await self.c.call("INCR", "n"), "QUEUED")
        self.assertEqual(await self.c.call("EXEC"), [1, 2])
        await self.c.call("MULTI")
        await self.c.call("INCR", "n")
        await self.c.call("DISCARD")
        self.assertEqual(await self.c.call("GET", "n"), "2")
        with self.assertRaises(CommandError):
            await self.c.call("EXEC")

    async def test_pub_sub(self):
        sub = await Client.connect("127.0.0.1", self.server.port)
        self.assertEqual(await sub.call("SUBSCRIBE", "news"), ["subscribe", "news", 1])
        self.assertEqual(await self.c.call("PUBLISH", "news", "hi"), 1)
        self.assertEqual(await asyncio.wait_for(sub.reply(), 2), ["message", "news", "hi"])
        self.assertEqual(await sub.call("UNSUBSCRIBE", "news"), ["unsubscribe", "news", 0])
        self.assertEqual(await self.c.call("PUBLISH", "news", "again"), 0)
        await sub.close()

    async def test_many_clients(self):
        clients = [await Client.connect("127.0.0.1", self.server.port) for _ in range(20)]
        await asyncio.gather(*[self._bump(c) for c in clients])
        self.assertEqual(await self.c.call("GET", "shared"), "200")
        for c in clients:
            await c.close()

    @staticmethod
    async def _bump(c):
        for _ in range(10):
            await c.call("INCR", "shared")

    async def test_log_rebuilds_data(self):
        await self.c.call("SET", "a", "1")
        await self.c.call("RPUSH", "l", "x", "y")
        await self.c.call("ZADD", "z", 2, "m")
        await self.c.call("DEL", "a")
        await self.c.call("HSET", "h", "f", "v")
        store = Store()
        self.assertGreater(replay(self.aof, store), 0)
        self.assertIsNone(store.execute(["GET", "a"]))
        self.assertEqual(store.execute(["LRANGE", "l", "0", "-1"]), ["x", "y"])
        self.assertEqual(store.execute(["ZSCORE", "z", "m"]), "2")

    async def test_rewrite_shrinks_the_log_and_keeps_data(self):
        for _ in range(100):
            await self.c.call("INCR", "n")
        await self.c.call("EXPIRE", "n", 1000)
        before = os.path.getsize(self.aof)
        await self.c.call("BGREWRITEAOF")
        self.assertLess(os.path.getsize(self.aof), before)
        await self.c.call("INCR", "n")
        store = Store()
        replay(self.aof, store)
        self.assertEqual(store.execute(["GET", "n"]), "101")
        self.assertGreater(store.execute(["TTL", "n"]), 900)


class TestLogDamage(unittest.TestCase):
    def test_half_written_record_is_dropped(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "a.aof")
        with open(p, "wb") as f:
            f.write(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb")
        s = Store()
        self.assertEqual(replay(p, s), 1)
        self.assertEqual(s.execute(["GET", "a"]), "1")
        self.assertEqual(os.path.getsize(p), 27)
        shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
