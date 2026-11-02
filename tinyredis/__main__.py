"""Start the server: python -m tinyredis [--port 6399] [--aof data.aof]"""
import argparse
import asyncio

from .server import Server


async def run(args):
    server = await Server(aof=args.aof, host=args.host, port=args.port).start()
    print(f"TinyRedis listening on {args.host}:{server.port}")
    await asyncio.Event().wait()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6399)
    p.add_argument("--aof", help="file for the append-only log")
    try:
        asyncio.run(run(p.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
