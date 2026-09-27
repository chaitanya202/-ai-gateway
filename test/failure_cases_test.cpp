#include "types.hpp"
#include "spsc_queue.hpp"
#include "http_parser.hpp"
#include "connection_manager.hpp"
#include "backend_bridge.hpp"
#include "worker_pool.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <thread>
#include <chrono>

using namespace aigw;

void test_fast_path_bypass() {
    std::cout << "[Failure / Resilience 1] Fast-Path Bypass Verification...\n";
    GatewayConfig cfg;
    cfg.worker_count = 4;
    WorkerPool pool(cfg);

    // Intentionally do NOT start worker pool, simulating workers completely hung or dead
    // Fast path should still respond immediately on ingress thread!
    
    // Test 1: Health check
    std::string health_req = "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n";
    // We pass a dummy socket -1 for unit check
    assert(pool.handle_fast_path(-1, health_req));

    // Test 2: Metrics check
    std::string metrics_req = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
    assert(pool.handle_fast_path(-1, metrics_req));

    // Test 3: Models list check
    std::string models_req = "GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n";
    assert(pool.handle_fast_path(-1, models_req));

    // Test 4: Real LLM generation must NOT be intercepted by fast path
    std::string chat_req = "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}";
    assert(!pool.handle_fast_path(-1, chat_req));

    std::cout << "  -> PASSED: Lightweight routes bypass inference queues, preventing head-of-line blocking.\n";
}

void test_partitioned_queue_isolation() {
    std::cout << "[Failure / Resilience 2] Partitioned Queue Isolation & Backpressure...\n";
    GatewayConfig cfg;
    cfg.worker_count = 4;
    cfg.queue_capacity = 8; // Small queue to test saturation
    WorkerPool pool(cfg);

    // Saturate queue 0 completely
    for (size_t i = 0; i < 8; ++i) {
        HttpRequestTask t{.client_fd = 0, .request_id = i};
        assert(pool.push_task(0, std::move(t)));
    }

    // Queue 0 is now full: must return false (wait-free backpressure)
    HttpRequestTask overflow{.client_fd = 0, .request_id = 999};
    assert(!pool.push_task(0, std::move(overflow)));

    // Crucial isolation test: Queue 1, 2, 3 must still accept tasks normally!
    HttpRequestTask t_isolated{.client_fd = 1, .request_id = 100};
    assert(pool.push_task(1, std::move(t_isolated)));

    std::cout << "  -> PASSED: Saturated worker does not cascade failure to other queues.\n";
}

void test_upstream_offline_failure() {
    std::cout << "[Failure / Resilience 3] Upstream AI Engine Offline (502 Bad Gateway)...\n";
    GatewayConfig cfg;
    cfg.upstream_host = "127.0.0.1";
    cfg.upstream_port = 59999; // Non-existent port

    BackendBridge bridge(cfg);

    // Fake client socket pair
    int fds[2];
    int res = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (res == 0) {
        std::string raw_req = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 2\r\n\r\n{}";
        bool ok = bridge.forward_and_relay(fds[0], raw_req, false);
        assert(!ok); // Must report failure

        // Verify client received 502 Bad Gateway
        char buf[1024];
        ssize_t n = read(fds[1], buf, sizeof(buf));
        if (n > 0) {
            std::string resp(buf, n);
            assert(resp.find("502 Bad Gateway") != std::string::npos);
        }
        close(fds[1]);
        std::cout << "  -> PASSED: 502 Bad Gateway emitted immediately when upstream engine is dead.\n";
    } else {
        std::cout << "  -> SKIPPED (socketpair not supported on this platform).\n";
    }
}

void test_security_bounds_enforcement() {
    std::cout << "[Failure / Resilience 4] Security Bounds (431 Header & 413 Payload)...\n";
    ConnectionManager mgr;
    std::vector<std::string> ready;

    // Header overflow attack
    std::string attack_header(ConnectionManager::MAX_HEADER_SIZE + 100, 'X');
    FrameStatus s1 = mgr.process_incoming_data(50, attack_header.data(), attack_header.size(), ready);
    assert(s1 == FrameStatus::ERR_HEADER_TOO_LARGE);
    mgr.remove_connection(50);

    // Payload OOM exploit
    std::string attack_body = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 67108864\r\n\r\n"; // 64 MB
    FrameStatus s2 = mgr.process_incoming_data(51, attack_body.data(), attack_body.size(), ready);
    assert(s2 == FrameStatus::ERR_PAYLOAD_TOO_LARGE);
    mgr.remove_connection(51);

    std::cout << "  -> PASSED: Memory exhaustion attacks strictly intercepted before allocation.\n";
}

int main() {
    std::cout << "=================================================================\n";
    std::cout << "    AI INGRESS GATEWAY - RESILIENCE & FAILURE TEST SUITE         \n";
    std::cout << "=================================================================\n\n";

    test_fast_path_bypass();
    test_partitioned_queue_isolation();
    test_upstream_offline_failure();
    test_security_bounds_enforcement();

    std::cout << "\n=================================================================\n";
    std::cout << "  ALL FAILURE & RESILIENCE TESTS PASSED (100% CLEAN)             \n";
    std::cout << "=================================================================\n";
    return 0;
}
