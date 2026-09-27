#pragma once

#include <cstdint>
#include <string>
#include <chrono>
#include <string_view>
#include <vector>

namespace aigw {

enum class HttpMethod {
    UNKNOWN,
    GET,
    POST,
    PUT,
    DELETE_,
    OPTIONS
};

struct HttpRequestTask {
    int client_fd = -1;
    uint64_t request_id = 0;
    std::chrono::steady_clock::time_point ingress_time;
    std::string raw_payload; // Entire raw HTTP request (headers + body)
};

struct ParsedRequest {
    HttpMethod method = HttpMethod::UNKNOWN;
    std::string path;
    std::string http_version;
    std::string api_key;
    std::string content_type;
    size_t content_length = 0;
    std::string_view body;
    bool is_streaming = false;
};

struct GatewayConfig {
    int listen_port = 8080;
    std::string listen_host = "0.0.0.0";
    size_t queue_capacity = 65536; // Must be power of 2
    
    // Upstream AI engine configuration (vLLM / Python)
    std::string upstream_host = "127.0.0.1";
    int upstream_port = 8000;
    std::string upstream_socket_path = "/tmp/vllm.sock";
    bool use_unix_socket = false;
    
    // Optional auth token to require (if non-empty, checks Bearer token)
    std::string required_api_key = "";

    // Web static root directory for dashboard
    std::string web_root = "web";

    // Number of partitioned lock-free worker threads and SPSC queues
    size_t worker_count = 8;
};

} // namespace aigw
