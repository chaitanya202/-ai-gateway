#!/usr/bin/env python3
"""
Mock vLLM / OpenAI Compatible Backend Server
Simulates local AI engine response generation for testing ingress gateway throughput and streaming SSE tokens.
"""

import sys
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

HOST = "127.0.0.1"
PORT = 8000

class MockVLLMHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/v1/models":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            resp = json.dumps({
                "object": "list",
                "data": [
                    {"id": "meta-llama/Llama-3-8B-Instruct", "object": "model", "owned_by": "vllm"}
                ]
            }).encode("utf-8")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
        else:
            self.send_error(404, "Not Found")

    def do_POST(self):
        if self.path == "/v1/chat/completions":
            content_length = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_length)
            
            try:
                payload = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                payload = {}

            is_streaming = payload.get("stream", False)
            model_name = payload.get("model", "local-llm")
            messages = payload.get("messages", [])
            last_prompt = messages[-1].get("content", "") if messages else "Hello"

            if is_streaming:
                # Stream tokens as Server-Sent Events (SSE)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()

                sample_tokens = ["Processed", " by", " Edge", "-Triggered", " epoll", " ->", " SPSC", " ring", " buffer", " ->", " vLLM!"]
                req_id = f"chatcmpl-{int(time.time()*1000)}"

                for token in sample_tokens:
                    chunk = {
                        "id": req_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model_name,
                        "choices": [{
                            "index": 0,
                            "delta": {"content": token},
                            "finish_reason": None
                        }]
                    }
                    data_line = f"data: {json.dumps(chunk)}\n\n"
                    self.wfile.write(data_line.encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(0.01) # Simulate token generation latency

                # Final finish chunk
                finish_chunk = {
                    "id": req_id,
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model_name,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop"
                    }]
                }
                self.wfile.write(f"data: {json.dumps(finish_chunk)}\n\n".encode("utf-8"))
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            else:
                # Standard buffered JSON response
                resp_data = {
                    "id": f"chatcmpl-{int(time.time()*1000)}",
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model_name,
                    "choices": [{
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": f"Echoing from vLLM backend: Received prompt '{last_prompt}' via low-latency SPSC ingress!"
                        },
                        "finish_reason": "stop"
                    }],
                    "usage": {
                        "prompt_tokens": 12,
                        "completion_tokens": 20,
                        "total_tokens": 32
                    }
                }
                body = json.dumps(resp_data).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        else:
            self.send_error(404, "Endpoint not found")

    def log_message(self, format, *args):
        # Concise logging
        sys.stderr.write(f"[Mock-vLLM] {self.address_string()} - {format%args}\n")

def run():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    server = HTTPServer((HOST, port), MockVLLMHandler)
    print(f"[Mock-vLLM] Listening on http://{HOST}:{port} (Ready for gateway ingress traffic)...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Mock-vLLM] Shutting down.")
        server.server_close()

if __name__ == "__main__":
    run()
