# Round-robin TCP proxy (Python standard library only).
#
# It copies raw bytes in both directions, so the client sees an EXACT copy of the
# backend server: every endpoint, streaming, headers, errors - nothing is parsed or rewritten.
# Each new connection goes to the next backend. If a backend is down (refused / timeout)
# it is skipped and the next one is tried, so you can list both ports even while one is busy.
#
# Run:      python3 rr_proxy_tcp.py
# Override: python3 rr_proxy_tcp.py 8000 127.0.0.1:30000 127.0.0.1:30001
import asyncio, itertools, resource, sys

LISTEN_PORT = 8000
BACKENDS = [
    # ("127.0.0.1", 30000),   # qwen-a  (uncomment when it is free)
    ("127.0.0.1", 30001),     # qwen-b
]

if len(sys.argv) > 1:
    LISTEN_PORT = int(sys.argv[1])
if len(sys.argv) > 2:
    BACKENDS = [(a.rsplit(":", 1)[0], int(a.rsplit(":", 1)[1])) for a in sys.argv[2:]]

_counter = itertools.count()


async def _pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass


async def handle(creader, cwriter):
    start = next(_counter)
    ureader = uwriter = None
    for i in range(len(BACKENDS)):
        host, port = BACKENDS[(start + i) % len(BACKENDS)]
        try:
            ureader, uwriter = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10)
            break
        except Exception:
            continue
    if uwriter is None:
        cwriter.close()
        return

    t1 = asyncio.create_task(_pipe(creader, uwriter))
    t2 = asyncio.create_task(_pipe(ureader, cwriter))
    await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for t in (t1, t2):
        t.cancel()
    for w in (cwriter, uwriter):
        try:
            w.close()
        except Exception:
            pass


async def main():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass
    print(f"open-files limit: {resource.getrlimit(resource.RLIMIT_NOFILE)[0]} (was {soft})")
    server = await asyncio.start_server(handle, "0.0.0.0", LISTEN_PORT, backlog=4096)
    print(f"Proxy on :{LISTEN_PORT} -> {BACKENDS}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
