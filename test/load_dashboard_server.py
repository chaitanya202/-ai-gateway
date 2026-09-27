#!/usr/bin/env python3
"""
Integrated WebUI Dashboard & Concurrent Load Generator Server
Serves the telemetry WebUI on http://localhost:3000, bridges live C++ gateway metrics,
and launches multi-threaded stress tests on demand.
"""

import sys
import os
import json
import time
import urllib.request
import urllib.error
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

PORT = 3000
GATEWAY_URL = "http://127.0.0.1:8080"
WEB_DIR = Path(__file__).parent.parent / "web"

# Load generator state
load_state = {
    "running": False,
    "concurrency": 100,
    "qps": 2500,
    "mode": "json",
    "stop_event": threading.Event(),
    "worker_threads": []
}

class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.path = "/dashboard.html"
            return super().do_GET()
        elif self.path == "/api/metrics":
            self.handle_get_metrics()
        else:
            return super().do_GET()

    def do_POST(self):
        if self.path == "/api/load/start":
            self.handle_load_start()
        elif self.path == "/api/load/stop":
            self.handle_load_stop()
        else:
            self.send_error(404, "Not Found")

    def handle_get_metrics(self):
        # Query real C++ gateway if active
        data = None
        try:
            req = urllib.request.Request(f"{GATEWAY_URL}/metrics", headers={"User-Agent": "DashboardServer/1.0"})
            with urllib.request.urlopen(req, timeout=0.5) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode())
        except Exception:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if data:
            self.wfile.write(json.dumps(data).encode("utf-8"))
        else:
            # Fallback signal indicating dashboard simulator mode
            self.wfile.write(b"null")

    def handle_load_start(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            payload = {}

        concurrency = int(payload.get("concurrency", 50))
        mode = payload.get("mode", "json")
        
        start_stress_test(concurrency, mode)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "started", "concurrency": concurrency, "mode": mode}).encode("utf-8"))

    def handle_load_stop(self):
        stop_stress_test()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "stopped"}).encode("utf-8"))

    def log_message(self, format, *args):
        # Suppress noisy HTTP asset logs
        return

def worker_load_loop(stop_event, is_streaming):
    payload = {
        "model": "meta-llama/Llama-3-8B-Instruct",
        "messages": [{"role": "user", "content": "Benchmark prompt for Edge-Triggered epoll reactor."}],
        "stream": is_streaming
    }
    encoded = json.dumps(payload).encode("utf-8")

    while not stop_event.is_set():
        try:
            req = urllib.request.Request(
                f"{GATEWAY_URL}/v1/chat/completions",
                data=encoded,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=1.0) as resp:
                resp.read()
        except Exception:
            time.sleep(0.01)

def start_stress_test(concurrency, mode):
    stop_stress_test()
    load_state["running"] = True
    load_state["stop_event"].clear()
    load_state["worker_threads"] = []

    is_streaming = (mode == "sse")
    for _ in range(concurrency):
        t = threading.Thread(target=worker_load_loop, args=(load_state["stop_event"], is_streaming), daemon=True)
        t.start()
        load_state["worker_threads"].append(t)
    print(f"[LoadGenerator] Launched {concurrency} concurrent stress worker threads (Streaming: {is_streaming}).")

def stop_stress_test():
    if load_state["running"]:
        load_state["stop_event"].set()
        load_state["running"] = False
        load_state["worker_threads"] = []
        print("[LoadGenerator] All stress worker threads stopped.")

def run():
    server = HTTPServer(("0.0.0.0", PORT), DashboardRequestHandler)
    print("==================================================================")
    print("  AI Ingress Gateway | Telemetry Dashboard & Load Profiler  ")
    print("==================================================================")
    print(f" Dashboard Web UI URL:  http://localhost:{PORT}")
    print(f" Target C++ Gateway:    {GATEWAY_URL}")
    print("==================================================================")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        stop_stress_test()
        print("\n[Dashboard] Shutting down.")
        server.server_close()

if __name__ == "__main__":
    run()
