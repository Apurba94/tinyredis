# TinyRedis

An in-memory data server written from scratch in pure Python. It speaks the RESP protocol, so `redis-cli` and ordinary Redis client libraries can talk to it for the commands it supports.

```
python -m tinyredis --port 6399 --aof data.aof
```

```python
import asyncio
from tinyredis import Client

async def main():
    c = await Client.connect("127.0.0.1", 6399)
    await c.call("SET", "visits", 0)
    await c.call("INCR", "visits")
    print(await c.call("GET", "visits"))

asyncio.run(main())
```

## What it supports

| Area | Commands |
|---|---|
| Strings | `SET` (with `EX`, `PX`, `NX`, `XX`, `GET`), `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE` |
| Keys | `DEL`, `EXISTS`, `KEYS`, `TYPE`, `RENAME`, `DBSIZE`, `FLUSHALL` |
| Expiry | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `TTL`, `PTTL`, `PERSIST` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LLEN` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`, `SINTER`, `SUNION`, `SDIFF` |
| Hashes | `HSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HINCRBY` |
| Sorted sets | `ZADD`, `ZSCORE`, `ZREM`, `ZCARD`, `ZRANK`, `ZINCRBY`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE` |
| Messaging | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |
| Persistence | Append-only log, and `BGREWRITEAOF` to compact it |

## How it is built

- `store.py` holds the data and all commands, and takes the clock as a parameter, so expiry is tested without waiting.
- `server.py` handles sockets, the protocol, pub/sub, transactions and the log. Each connection runs on one event loop, so commands never overlap.
- Expired keys are removed when read, and by a sampling sweep that catches keys nobody reads.
- The log uses the same format as the network protocol. A half-written last record, as left by a crash, is dropped on replay.

## Limits

It has no replication, clustering or authentication, so keep it on localhost. `BGREWRITEAOF` runs in the foreground despite its name. It stores text, not arbitrary binary values. Its `KEYS` and set commands return sorted output, which real Redis does not promise.

## Tests

```
python -m unittest discover -s tests
```

MIT licence.
