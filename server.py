"""
Bhati March 2026 Phase 2 — Railway Server
Endpoints: GET /data.json, GET /health, GET /stats, POST /refresh
           GET /download/scans, GET /download/users
"""
import threading
import time
import json
import os
import gzip
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone

_BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE  = os.path.join(_BASE_DIR, "data.json")

# ── Persistent volume paths (Railway Volume mounted at /data) ──
SCANS_EXCEL = os.environ.get("SCANS_EXCEL_PATH", "/data/scans_cache.xlsx")
USERS_EXCEL = os.environ.get("USERS_EXCEL_PATH", "/data/user_cache.xlsx")

FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", "30"))  # seconds

# Shared state
_state = {
    "last_fetch_at":    None,
    "last_fetch_status": "pending",
    "fetch_count":      0,
    "start_time":       datetime.now(timezone.utc).isoformat(),
    "force_refresh":    False,
}
_state_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/data.json"):
            self._serve_json()
        elif path == "/health":
            self._send(200, "OK", "text/plain")
        elif path == "/stats":
            self._serve_stats()
        elif path == "/download/scans":
            self._serve_file(SCANS_EXCEL, "scans_cache.xlsx")
        elif path == "/download/users":
            self._serve_file(USERS_EXCEL, "user_cache.xlsx")
        else:
            self._send(404, "Not found", "text/plain")

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/refresh":
            with _state_lock:
                _state["force_refresh"] = True
            self._send(200, '{"ok":true,"message":"Refresh triggered"}', "application/json")
        else:
            self._send(404, "Not found", "text/plain")

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _serve_json(self):
        if not os.path.exists(DATA_FILE):
            self._send(503, '{"error":"Data not ready yet — cold start in progress, retry in 10s"}',
                       "application/json")
            return

        with open(DATA_FILE, "rb") as f:
            raw = f.read()

        # Gzip compress if client accepts it (saves ~80% bandwidth)
        accept_enc = self.headers.get("Accept-Encoding", "")
        if "gzip" in accept_enc:
            compressed = gzip.compress(raw, compresslevel=6)
            self.send_response(200)
            self.send_header("Content-Type",        "application/json")
            self.send_header("Content-Encoding",    "gzip")
            self.send_header("Content-Length",      str(len(compressed)))
            self.send_header("Cache-Control",       "no-cache, no-store")
            self._cors()
            self.end_headers()
            self.wfile.write(compressed)
        else:
            self.send_response(200)
            self.send_header("Content-Type",   "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control",  "no-cache, no-store")
            self._cors()
            self.end_headers()
            self.wfile.write(raw)

    def _serve_stats(self):
        file_size = os.path.getsize(DATA_FILE) if os.path.exists(DATA_FILE) else 0
        scans_size = os.path.getsize(SCANS_EXCEL) if os.path.exists(SCANS_EXCEL) else 0
        with _state_lock:
            stats = {
                "status":            _state["last_fetch_status"],
                "last_fetch_at":     _state["last_fetch_at"],
                "fetch_count":       _state["fetch_count"],
                "server_started_at": _state["start_time"],
                "uptime_seconds":    int((datetime.now(timezone.utc) -
                                         datetime.fromisoformat(_state["start_time"])).total_seconds()),
                "data_file_kb":      round(file_size / 1024, 1),
                "scans_excel_kb":    round(scans_size / 1024, 1),
                "fetch_interval_s":  FETCH_INTERVAL,
                "collection":        "bhati-march-2026-ph2",
                "excel_paths": {
                    "scans": SCANS_EXCEL,
                    "users": USERS_EXCEL,
                }
            }
        self._send(200, json.dumps(stats), "application/json")

    def _serve_file(self, filepath, filename):
        """Serve an Excel file as a download."""
        if not os.path.exists(filepath):
            self._send(404, f'{{"error":"File not found at {filepath} — may not have data yet"}}',
                       "application/json")
            return
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(data)

    def _send(self, code, body, ctype):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type",   ctype)
        self.send_header("Content-Length", str(len(b)))
        self._cors()
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, format, *args):
        # Only log non-200 responses
        status = args[1] if len(args) > 1 else "000"
        if not str(status).startswith("2"):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] HTTP {format % args}")


def fetch_loop():
    import fetch_data

    print(f"[{_ts()}] Cold start render...")
    try:
        fetch_data.cold_start_render()
        with _state_lock:
            _state["last_fetch_status"] = "ok"
            _state["last_fetch_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        print(f"[{_ts()}] Cold start error: {e}")
        with _state_lock:
            _state["last_fetch_status"] = f"error: {e}"

    print(f"[{_ts()}] Fetch loop started — every {FETCH_INTERVAL}s")

    while True:
        # Check for force refresh or wait for interval
        elapsed = 0
        while elapsed < FETCH_INTERVAL:
            time.sleep(1)
            elapsed += 1
            with _state_lock:
                if _state["force_refresh"]:
                    _state["force_refresh"] = False
                    print(f"[{_ts()}] Force refresh triggered")
                    break

        try:
            print(f"[{_ts()}] Fetching...")
            fetch_data.main()
            with _state_lock:
                _state["last_fetch_status"] = "ok"
                _state["last_fetch_at"]     = datetime.now(timezone.utc).isoformat()
                _state["fetch_count"]      += 1
            print(f"[{_ts()}] Fetch complete.")
        except Exception as e:
            print(f"[{_ts()}] Fetch error: {e}")
            with _state_lock:
                _state["last_fetch_status"] = f"error: {e}"


def _ts():
    return datetime.now().strftime("%H:%M:%S")


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 8080))

    t = threading.Thread(target=fetch_loop, daemon=True)
    t.start()

    print(f"Server running on port {PORT}")
    print(f"Excel paths — scans: {SCANS_EXCEL} | users: {USERS_EXCEL}")
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
