# Tiny round-robin proxy (Python standard library only).
# Run:  python3 rr_proxy.py      -> listens on port 8000, alternates 30000 / 30001
import itertools, threading, http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BACKENDS = [("127.0.0.1", 30000), ("127.0.0.1", 30001)]
LISTEN_PORT = 8000

_cycle = itertools.cycle(BACKENDS)
_lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _proxy(self):
        with _lock:
            host, port = next(_cycle)
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else None
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in ("host", "connection", "content-length")}
        try:
            conn = http.client.HTTPConnection(host, port, timeout=3600)
            conn.request(self.command, self.path, body=body, headers=headers)
            r = conn.getresponse()
            data = r.read()
            conn.close()
            self.send_response(r.status)
            for k, v in r.getheaders():
                if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            msg = f"proxy error: {e}".encode()
            self.send_response(502)
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    do_GET = do_POST = _proxy

    def log_message(self, *a):
        pass

class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 2048

if __name__ == "__main__":
    print(f"Proxy on :{LISTEN_PORT} -> {BACKENDS}")
    Server(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()
