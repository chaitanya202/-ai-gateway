#include "worker_pool.hpp"
#include "http_parser.hpp"
#include "metrics.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CPU_PAUSE() _mm_pause()
#else
#define CPU_PAUSE() std::this_thread::yield()
#endif

namespace aigw {

WorkerPool::WorkerPool(const GatewayConfig& config)
    : config_(config), worker_count_(config.worker_count > 0 ? config.worker_count : 4) {
    queues_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i) {
        queues_.push_back(std::make_unique<SpscQueue<HttpRequestTask>>(config.queue_capacity));
    }
}

WorkerPool::~WorkerPool() {
    stop();
}

void WorkerPool::start() {
    if (!running_.exchange(true)) {
        workers_.reserve(worker_count_);
        for (size_t i = 0; i < worker_count_; ++i) {
            workers_.emplace_back(&WorkerPool::worker_loop, this, i);
        }
        std::cout << "[WorkerPool] Spawned " << worker_count_ 
                  << " partitioned lock-free worker threads.\n";
    }
}

void WorkerPool::stop() {
    if (running_.exchange(false)) {
        for (auto& w : workers_) {
            if (w.joinable()) {
                w.join();
            }
        }
        workers_.clear();
    }
}

bool WorkerPool::handle_fast_path(int client_fd, const std::string& raw_payload) {
    std::string_view raw(raw_payload);

    // 1. Health check fast path
    if (raw.starts_with("GET /healthz") || raw.starts_with("GET /health")) {
        std::string body = "{\"status\": \"healthy\", \"service\": \"aigw-ingress\"}";
        std::string resp = HttpParser::response_200_json(body);
        ::write(client_fd, resp.data(), resp.size());
        ::close(client_fd);
        return true;
    }

    // 2. Metrics & telemetry fast path
    if (raw.starts_with("GET /metrics") || raw.starts_with("GET /api/metrics") || raw.starts_with("GET /api/stats")) {
        MetricsCollector::instance().set_queue_occupancy(total_queue_size(), total_queue_capacity());
        std::string metrics_json = MetricsCollector::instance().to_json();
        std::string resp = HttpParser::response_200_json(metrics_json);
        ::write(client_fd, resp.data(), resp.size());
        ::close(client_fd);
        return true;
    }

    // 3. Models list fast path (never blocks behind slow LLM generations)
    if (raw.starts_with("GET /v1/models")) {
        std::string body = "{\"object\":\"list\",\"data\":[{\"id\":\"meta-llama/Llama-3-8B-Instruct\",\"object\":\"model\",\"owned_by\":\"vllm\"}]}";
        std::string resp = HttpParser::response_200_json(body);
        ::write(client_fd, resp.data(), resp.size());
        ::close(client_fd);
        return true;
    }

    // 4. Static web dashboard fast path
    if (raw.starts_with("GET / ") || raw.starts_with("GET /dashboard.html") || raw.starts_with("GET /index.html")) {
        std::string html_path = config_.web_root + "/dashboard.html";
        std::ifstream file(html_path);
        std::string content;
        if (file.is_open()) {
            std::stringstream buffer;
            buffer << file.rdbuf();
            content = buffer.str();
        } else {
            std::ifstream file2("web/dashboard.html");
            if (file2.is_open()) {
                std::stringstream buffer;
                buffer << file2.rdbuf();
                content = buffer.str();
            } else {
                content = "<html><body><h1>AI Ingress Gateway</h1><p>Dashboard file web/dashboard.html not found.</p></body></html>";
            }
        }
        std::string resp = HttpParser::build_response(200, "OK", "text/html; charset=utf-8", content);
        ::write(client_fd, resp.data(), resp.size());
        ::close(client_fd);
        return true;
    }

    // 5. CORS preflight fast path
    if (raw.starts_with("OPTIONS ")) {
        std::string cors_resp = HttpParser::build_response(200, "OK", "text/plain", "");
        ::write(client_fd, cors_resp.data(), cors_resp.size());
        ::close(client_fd);
        return true;
    }

    return false;
}

bool WorkerPool::push_task(size_t worker_idx, HttpRequestTask&& task) {
    if (worker_idx >= worker_count_) {
        worker_idx = 0;
    }
    return queues_[worker_idx]->try_push(std::move(task));
}

size_t WorkerPool::total_queue_size() const noexcept {
    size_t total = 0;
    for (const auto& q : queues_) {
        total += q->size();
    }
    return total;
}

size_t WorkerPool::total_queue_capacity() const noexcept {
    return worker_count_ * config_.queue_capacity;
}

void WorkerPool::worker_loop(size_t worker_id) {
    HttpRequestTask task;
    BackendBridge bridge(config_);
    size_t consecutive_empty = 0;

    auto& queue = *queues_[worker_id];

    while (running_.load(std::memory_order_relaxed)) {
        if (queue.try_pop(task)) {
            consecutive_empty = 0;
            process_inference_task(task, bridge);
            total_processed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            ++consecutive_empty;
            if (consecutive_empty < 64) {
                CPU_PAUSE();
            } else if (consecutive_empty < 512) {
                std::this_thread::yield();
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }
    }

    // Drain remaining tasks on shutdown
    while (queue.try_pop(task)) {
        process_inference_task(task, bridge);
        total_processed_.fetch_add(1, std::memory_order_relaxed);
    }
}

void WorkerPool::process_inference_task(HttpRequestTask& task, BackendBridge& bridge) {
    if (task.client_fd < 0) return;

    ParsedRequest req;
    if (!HttpParser::parse(task.raw_payload, req)) {
        std::string err = HttpParser::response_400_bad_request("{\"error\": \"Malformed HTTP Request\"}");
        ::write(task.client_fd, err.data(), err.size());
        ::close(task.client_fd);
        return;
    }

    // Validate authorization if enforced
    if (!config_.required_api_key.empty() && req.api_key != config_.required_api_key) {
        std::string auth_err = HttpParser::response_401_unauthorized(
            "{\"error\": {\"message\": \"Incorrect API key provided.\", \"type\": \"invalid_request_error\"}}");
        ::write(task.client_fd, auth_err.data(), auth_err.size());
        ::close(task.client_fd);
        return;
    }

    // Forward inference request to upstream engine
    if (req.path.starts_with("/v1/")) {
        bridge.forward_and_relay(task.client_fd, task.raw_payload, req.is_streaming);

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - task.ingress_time).count();
        MetricsCollector::instance().record_request_completed(static_cast<uint64_t>(duration));
    } else {
        std::string not_found = HttpParser::response_404_not_found(
            "{\"error\": {\"message\": \"Unknown endpoint. Supported: /v1/*\", \"type\": \"invalid_request_error\"}}");
        ::write(task.client_fd, not_found.data(), not_found.size());
        ::close(task.client_fd);
    }
}

} // namespace aigw
