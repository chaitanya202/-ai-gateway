#!/usr/bin/env python3
"""
Benchmark and validation client for the C++ OpenAI Ingress Gateway.
Tests concurrency, edge-triggered accumulation, streaming SSE tokens, and latency.
"""

import sys
import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
import statistics

GATEWAY_URL = "http://127.0.0.1:8080"

def test_health():
    print("[Test] Verifying Gateway Health Endpoint...")
    req = urllib.request.Request(f"{GATEWAY_URL}/healthz", method="GET")
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode())
        print(f"  -> Health OK: {data}")

def send_chat_completion(req_id: int, stream: bool = False, api_key: str = None):
    payload = {
        "model": "meta-llama/Llama-3-8B-Instruct",
        "messages": [
            {"role": "user", "content": f"Request #{req_id}: High throughput test"}
        ],
        "stream": stream
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{GATEWAY_URL}/v1/chat/completions",
        data=data,
        headers={"Content-Type": "application/json"}
    )
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")

    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            if stream:
                # Read SSE stream
                chunks = []
                for line in resp:
                    chunks.append(line)
                body = b"".join(chunks)
            else:
                body = resp.read()
            t1 = time.perf_counter()
            return True, (t1 - t0) * 1000.0, len(body)
    except Exception as e:
        t1 = time.perf_counter()
        return False, (t1 - t0) * 1000.0, str(e)

def run_concurrency_benchmark(num_requests=500, concurrency=50, stream=False):
    mode = "Streaming SSE" if stream else "Buffered JSON"
    print(f"\n[Benchmark] Running {num_requests} requests (Concurrency: {concurrency}, Mode: {mode})...")

    latencies = []
    successes = 0
    failures = 0

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_chat_completion, i, stream) for i in range(num_requests)]
        for f in futures:
            ok, lat, _ = f.result()
            latencies.append(lat)
            if ok:
                successes += 1
            else:
                failures += 1

    t_total = time.perf_counter() - t_start
    rps = num_requests / t_total

    print("========================================")
    print(f" Total Requests:    {num_requests}")
    print(f" Success:           {successes} ({(successes/num_requests)*100:.1f}%)")
    print(f" Failures:          {failures}")
    print(f" Total Wall Time:   {t_total:.2f} s")
    print(f" Throughput:        {rps:.1f} req/sec")
    if latencies:
        print(f" Latency Min:       {min(latencies):.2f} ms")
        print(f" Latency P50:       {statistics.median(latencies):.2f} ms")
        sorted_lat = sorted(latencies)
        p95 = sorted_lat[int(0.95 * len(sorted_lat))]
        p99 = sorted_lat[int(0.99 * len(sorted_lat))]
        print(f" Latency P95:       {p95:.2f} ms")
        print(f" Latency P99:       {p99:.2f} ms")
        print(f" Latency Max:       {max(latencies):.2f} ms")
    print("========================================")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        test_health()
    else:
        test_health()
        run_concurrency_benchmark(num_requests=200, concurrency=20, stream=False)
        run_concurrency_benchmark(num_requests=100, concurrency=10, stream=True)
