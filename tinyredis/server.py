"""The network side: RESP protocol, client sessions, pub/sub, transactions and the log."""
import asyncio
import os

from .store import OK, WRITES, CommandError, Simple, Store


class ProtocolError(Exception):
    pass


def parse_frame(buf):
    """Read one command from bytes. Returns (args, bytes used), or (None, 0) if incomplete."""
    if not buf:
        return None, 0
    if buf[:1] != b"*":
        end = buf.find(b"\r\n")
        if end < 0:
            return None, 0
        return buf[:end].decode("utf-8", "replace").split(), end + 2
    end = buf.find(b"\r\n")
    if end < 0:
        return None, 0
    try:
        count = int(buf[1:end])
    except ValueError:
        raise ProtocolError("invalid multibulk length") from None
    pos = end + 2
    args = []
    for _ in range(count):
        end = buf.find(b"\r\n", pos)
        if end < 0:
            return None, 0
        if buf[pos:pos + 1] != b"$":
            raise ProtocolError("expected '$'")
        try:
            size = int(buf[pos + 1:end])
        except ValueError:
            raise ProtocolError("invalid bulk length") from None
        start = end + 2
        if len(buf) < start + size + 2:
            return None, 0
        args.append(buf[start:start + size].decode("utf-8", "replace"))
        pos = start + size + 2
    return args, pos


def encode(value):
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, Exception):
        return f"-{value}\r\n".encode()
    if isinstance(value, Simple):
        return f"+{value}\r\n".encode()
    if isinstance(value, bool):
        return f":{int(value)}\r\n".encode()
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, (list, tuple)):
        return f"*{len(value)}\r\n".encode() + b"".join(encode(v) for v in value)
    data = str(value).encode()
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


def encode_command(args):
    return encode([str(a) for a in args])


class Log:
    """Append-only file of write commands, in the same format as the network protocol."""

    def __init__(self, path):
        self.path = path
        self.f = open(path, "ab")
        self.bytes = os.path.getsize(path)

    def append(self, args):
        data = encode_command(args)
        self.f.write(data)
        self.f.flush()
        self.bytes += len(data)

    def close(self):
        self.f.close()


def replay(path, store):
    """Rebuild data from a log, stopping cleanly at a half-written last record."""
    if not os.path.exists(path):
        return 0
    with open(path, "rb") as f:
        data = f.read()
    pos = done = 0
    while pos < len(data):
        args, used = parse_frame(data[pos:])
        if not args:
            break
        try:
            store.execute(args)
        except CommandError:
            pass
        pos += used
        done += 1
    if pos < len(data):
        with open(path, "r+b") as f:
            f.truncate(pos)
    return done


class Server:
    def __init__(self, store=None, aof=None, host="127.0.0.1", port=6399):
        self.store = store or Store()
        self.host, self.port = host, port
        self.channels = {}
        self.aof_path = aof
        self.log = None
        if aof:
            replay(aof, self.store)
            self.log = Log(aof)
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(self.client, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        self.sweeper = asyncio.create_task(self._sweep())
        return self

    async def _sweep(self):
        while True:
            await asyncio.sleep(0.1)
            self.store.sweep()

    async def stop(self):
        self.sweeper.cancel()
        self.server.close()
        await self.server.wait_closed()
        if self.log:
            self.log.close()

    def rewrite_log(self):
        """Replace the log with the shortest form of the current data."""
        if not self.log:
            raise CommandError("ERR append-only log is off")
        self.log.close()
        tmp = self.aof_path + ".tmp"
        with open(tmp, "wb") as f:
            for c in self.store.dump_commands():
                f.write(encode_command(c))
        os.replace(tmp, self.aof_path)
        self.log = Log(self.aof_path)
        return Simple("Background append only file rewriting started")

    def publish(self, channel, message):
        subs = self.channels.get(channel, ())
        for w in list(subs):
            try:
                w.write(encode([ "message", channel, message]))
            except ConnectionError:
                subs.discard(w)
        return len(subs)

    def run(self, args):
        """Execute one data command and record it if it changed anything."""
        result = self.store.execute(args)
        if self.log and args[0].upper() in WRITES:
            self.log.append(args)
        return result

    async def client(self, reader, writer):
        buf = b""
        subscribed = set()
        queue = None
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    return
                buf += chunk
                while buf:
                    try:
                        args, used = parse_frame(buf)
                    except ProtocolError as exc:
                        writer.write(encode(Exception(f"ERR Protocol error: {exc}")))
                        await writer.drain()
                        return
                    if args is None:
                        break
                    buf = buf[used:]
                    if not args:
                        continue
                    name = args[0].upper()
                    if name == "QUIT":
                        writer.write(encode(OK))
                        await writer.drain()
                        return
                    if name == "MULTI":
                        if queue is not None:
                            writer.write(encode(Exception("ERR MULTI calls can not be nested")))
                        else:
                            queue = []
                            writer.write(encode(OK))
                    elif name == "DISCARD":
                        writer.write(encode(OK if queue is not None else Exception("ERR DISCARD without MULTI")))
                        queue = None
                    elif name == "EXEC":
                        if queue is None:
                            writer.write(encode(Exception("ERR EXEC without MULTI")))
                        else:
                            replies = []
                            for c in queue:         # nothing else runs between these, as the store is single threaded
                                try:
                                    replies.append(self.run(c))
                                except CommandError as exc:
                                    replies.append(exc)
                            queue = None
                            writer.write(encode(replies))
                    elif queue is not None:
                        queue.append(args)
                        writer.write(encode(Simple("QUEUED")))
                    elif name == "SUBSCRIBE":
                        for ch in args[1:]:
                            self.channels.setdefault(ch, set()).add(writer)
                            subscribed.add(ch)
                            writer.write(encode(["subscribe", ch, len(subscribed)]))
                    elif name == "UNSUBSCRIBE":
                        for ch in (args[1:] or sorted(subscribed)):
                            self.channels.get(ch, set()).discard(writer)
                            subscribed.discard(ch)
                            writer.write(encode(["unsubscribe", ch, len(subscribed)]))
                    elif name == "PUBLISH" and len(args) == 3:
                        writer.write(encode(self.publish(args[1], args[2])))
                    elif name == "BGREWRITEAOF":
                        writer.write(encode(self.rewrite_log()))
                    else:
                        try:
                            writer.write(encode(self.run(args)))
                        except CommandError as exc:
                            writer.write(encode(exc))
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            for ch in subscribed:
                self.channels.get(ch, set()).discard(writer)
            writer.close()


class Client:
    """A small asyncio client, used by the tests and useful in scripts."""

    def __init__(self, reader, writer):
        self.r, self.w = reader, writer

    @classmethod
    async def connect(cls, host, port):
        return cls(*await asyncio.open_connection(host, port))

    async def reply(self):
        line = (await self.r.readline()).rstrip(b"\r\n")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise CommandError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = await self.r.readexactly(n + 2)
            return data[:-2].decode()
        if kind == b"*":
            return [await self.reply() for _ in range(int(rest))]
        raise ProtocolError(f"bad reply {line!r}")

    async def call(self, *args):
        self.w.write(encode_command(args))
        await self.w.drain()
        return await self.reply()

    async def close(self):
        self.w.close()
