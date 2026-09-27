#include "worker.hpp"
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

WorkerThread::WorkerThread(SpscQueue<HttpRequestTask>& queue, const GatewayConfig& config)
    : queue_(queue), config_(config), backend_bridge_(config) {}

WorkerThread::~WorkerThread() {
    stop();
}

void WorkerThread::start() {
    if (!running_.exchange(true)) {
        thread_ = std::thread(&WorkerThread::run, this);
    }
}

void WorkerThread::stop() {
    if (running_.exchange(false)) {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
}

void WorkerThread::run() {
    HttpRequestTask task;
    size_t consecutive_empty_polls = 0;

    while (running_.load(std::memory_order_relaxed)) {
        if (queue_.try_pop(task)) {
            consecutive_empty_polls = 0;
            process_task(task);
            total_processed_.fetch_add(1, std::memory_order_relaxed);
        } else {
            // Adaptive backoff: pause CPU for sub-microsecond busywait,
            // yield thread if idle for several cycles to prevent CPU starvation
            ++consecutive_empty_polls;
            if (consecutive_empty_polls < 64) {
                CPU_PAUSE();
            } else if (consecutive_empty_polls < 512) {
                std::this_thread::yield();
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }
    }

    // Drain remaining tasks before shutting down
    while (queue_.try_pop(task)) {
        process_task(task);
        total_processed_.fetch_add(1, std::memory_order_relaxed);
    }
}

void WorkerThread::process_task(HttpRequestTask& task) {
    if (task.client_fd < 0) return;

    ParsedRequest req;
    if (!HttpParser::parse(task.raw_payload, req)) {
        std::string err_resp = HttpParser::response_400_bad_request("{\"error\": \"Malformed HTTP Request\"}");
        ::write(task.client_fd, err_resp.data(), err_resp.size());
        ::close(task.client_fd);
        return;
    }

    // Handle CORS preflight
    if (req.method == HttpMethod::OPTIONS) {
        std::string cors_resp = HttpParser::build_response(200, "OK", "text/plain", "");
        ::write(task.client_fd, cors_resp.data(), cors_resp.size());
        ::close(task.client_fd);
        return;
    }

    // Handle Gateway Health Check
    if (req.path == "/healthz" || req.path == "/health") {
        std::string body = "{\"status\": \"healthy\", \"service\": \"aigw-ingress\"}";
        std::string health_resp = HttpParser::response_200_json(body);
        ::write(task.client_fd, health_resp.data(), health_resp.size());
        ::close(task.client_fd);
        return;
    }

    // Handle Gateway Metrics & Profiling Endpoint
    if (req.path == "/metrics" || req.path == "/api/stats" || req.path == "/api/metrics") {
        MetricsCollector::instance().set_queue_occupancy(queue_.size(), queue_.capacity());
        std::string metrics_json = MetricsCollector::instance().to_json();
        std::string metrics_resp = HttpParser::response_200_json(metrics_json);
        ::write(task.client_fd, metrics_resp.data(), metrics_resp.size());
        ::close(task.client_fd);
        return;
    }

    // Handle Static Web Dashboard directly from C++
    if (req.method == HttpMethod::GET && (req.path == "/" || req.path == "/dashboard.html" || req.path == "/index.html")) {
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
        ::write(task.client_fd, resp.data(), resp.size());
        ::close(task.client_fd);
        return;
    }

    // Authenticate API key if required
    if (!config_.required_api_key.empty() && req.api_key != config_.required_api_key) {
        std::string auth_err = HttpParser::response_401_unauthorized(
            "{\"error\": {\"message\": \"Incorrect API key provided.\", \"type\": \"invalid_request_error\"}}");
        ::write(task.client_fd, auth_err.data(), auth_err.size());
        ::close(task.client_fd);
        return;
    }

    // Route OpenAI endpoints
    if (req.path.starts_with("/v1/")) {
        backend_bridge_.forward_and_relay(task.client_fd, task.raw_payload, req.is_streaming);
        
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
