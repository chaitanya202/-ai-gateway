#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

struct BenchmarkResult {
    uint64_t total_requests = 0;
    uint64_t success_count = 0;
    uint64_t failure_count = 0;
    double elapsed_seconds = 0.0;
    double throughput_rps = 0.0;
    double latency_min_ms = 0.0;
    double latency_p50_ms = 0.0;
    double latency_p90_ms = 0.0;
    double latency_p95_ms = 0.0;
    double latency_p99_ms = 0.0;
    double latency_max_ms = 0.0;
    double latency_avg_ms = 0.0;
};

bool send_request(const std::string& host, int port, const std::string& http_request, double& latency_ms) {
    auto t0 = std::chrono::high_resolution_clock::now();

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    // 2 second timeout
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock);
        return false;
    }

    // Write request
    size_t total_sent = 0;
    while (total_sent < http_request.size()) {
        ssize_t n = write(sock, http_request.data() + total_sent, http_request.size() - total_sent);
        if (n <= 0) {
            close(sock);
            return false;
        }
        total_sent += n;
    }

    // Read full response until EOF
    char buf[4096];
    std::string response;
    while (true) {
        ssize_t n = read(sock, buf, sizeof(buf));
        if (n <= 0) break;
        response.append(buf, n);
    }
    close(sock);

    auto t1 = std::chrono::high_resolution_clock::now();
    latency_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // Verify HTTP 200 OK
    return (response.find("200 OK") != std::string::npos);
}

void print_report(const BenchmarkResult& res, int concurrency, bool is_streaming) {
    std::cout << "\n"
              << "=================================================================\n"
              << "          C++ INGRESS GATEWAY BENCHMARK REPORT                   \n"
              << "=================================================================\n"
              << " Concurrency Level:    " << concurrency << " parallel workers\n"
              << " Response Mode:        " << (is_streaming ? "Streaming SSE (stream: true)" : "Buffered JSON") << "\n"
              << " Total Requests:       " << res.total_requests << "\n"
              << " Successful Requests:  " << res.success_count << " ("
              << (100.0 * res.success_count / (res.total_requests ? res.total_requests : 1)) << "%)\n"
              << " Failed Requests:      " << res.failure_count << "\n"
              << " Total Wall Time:      " << res.elapsed_seconds << " s\n"
              << "-----------------------------------------------------------------\n"
              << " Throughput (CPS/QPS): " << res.throughput_rps << " req/sec\n"
              << "-----------------------------------------------------------------\n"
              << " Latency Profile (End-to-End):\n"
              << "   Min:                " << res.latency_min_ms << " ms\n"
              << "   P50 (Median):       " << res.latency_p50_ms << " ms\n"
              << "   P90:                " << res.latency_p90_ms << " ms\n"
              << "   P95:                " << res.latency_p95_ms << " ms\n"
              << "   P99:                " << res.latency_p99_ms << " ms\n"
              << "   Max:                " << res.latency_max_ms << " ms\n"
              << "   Average:            " << res.latency_avg_ms << " ms\n"
              << "=================================================================\n\n";
}

int main(int argc, char* argv[]) {
    int concurrency = 50;
    uint64_t total_requests = 2000;
    std::string host = "127.0.0.1";
    int port = 8080;
    bool is_streaming = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" && i + 1 < argc) concurrency = std::stoi(argv[++i]);
        else if (arg == "-n" && i + 1 < argc) total_requests = std::stoull(argv[++i]);
        else if (arg == "-h" && i + 1 < argc) host = argv[++i];
        else if (arg == "-p" && i + 1 < argc) port = std::stoi(argv[++i]);
        else if (arg == "-s") is_streaming = true;
        else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "  -c <concurrency>    Number of concurrent client threads (default: 50)\n"
                      << "  -n <num_requests>   Total requests to send (default: 2000)\n"
                      << "  -h <host>           Gateway host (default: 127.0.0.1)\n"
                      << "  -p <port>           Gateway port (default: 8080)\n"
                      << "  -s                  Test streaming SSE mode (stream: true)\n";
            return 0;
        }
    }

    std::cout << "[ai_stress] Launching C++ load test against " << host << ":" << port
              << " with " << concurrency << " workers (" << total_requests << " total requests)...\n";

    // Build raw HTTP request payload
    std::string json_body = is_streaming 
        ? "{\"model\":\"meta-llama/Llama-3-8B-Instruct\",\"messages\":[{\"role\":\"user\",\"content\":\"C++ stress benchmark prompt\"}],\"stream\":true}"
        : "{\"model\":\"meta-llama/Llama-3-8B-Instruct\",\"messages\":[{\"role\":\"user\",\"content\":\"C++ stress benchmark prompt\"}],\"stream\":false}";

    std::string http_req = "POST /v1/chat/completions HTTP/1.1\r\n"
                           "Host: " + host + ":" + std::to_string(port) + "\r\n"
                           "Content-Type: application/json\r\n"
                           "Content-Length: " + std::to_string(json_body.size()) + "\r\n"
                           "Connection: close\r\n\r\n" + json_body;

    std::atomic<uint64_t> requests_dispatched{0};
    std::atomic<uint64_t> successful_requests{0};
    std::atomic<uint64_t> failed_requests{0};

    std::vector<std::vector<double>> thread_latencies(concurrency);
    for (auto& vec : thread_latencies) {
        vec.reserve(total_requests / concurrency + 10);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> workers;
    for (int t = 0; t < concurrency; ++t) {
        workers.emplace_back([&, t]() {
            while (true) {
                uint64_t req_id = requests_dispatched.fetch_add(1, std::memory_order_relaxed);
                if (req_id >= total_requests) break;

                double lat_ms = 0.0;
                bool ok = send_request(host, port, http_req, lat_ms);
                if (ok) {
                    successful_requests.fetch_add(1, std::memory_order_relaxed);
                    thread_latencies[t].push_back(lat_ms);
                } else {
                    failed_requests.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& w : workers) {
        w.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double total_wall_sec = std::chrono::duration<double>(end_time - start_time).count();

    // Flatten and analyze latencies
    std::vector<double> all_latencies;
    all_latencies.reserve(total_requests);
    for (const auto& vec : thread_latencies) {
        all_latencies.insert(all_latencies.end(), vec.begin(), vec.end());
    }

    BenchmarkResult result;
    result.total_requests = total_requests;
    result.success_count = successful_requests.load();
    result.failure_count = failed_requests.load();
    result.elapsed_seconds = total_wall_sec;
    result.throughput_rps = total_requests / (total_wall_sec > 0 ? total_wall_sec : 1);

    if (!all_latencies.empty()) {
        std::sort(all_latencies.begin(), all_latencies.end());
        result.latency_min_ms = all_latencies.front();
        result.latency_max_ms = all_latencies.back();
        result.latency_p50_ms = all_latencies[static_cast<size_t>(all_latencies.size() * 0.50)];
        result.latency_p90_ms = all_latencies[static_cast<size_t>(all_latencies.size() * 0.90)];
        result.latency_p95_ms = all_latencies[static_cast<size_t>(all_latencies.size() * 0.95)];
        result.latency_p99_ms = all_latencies[static_cast<size_t>(all_latencies.size() * 0.99)];
        double sum = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0);
        result.latency_avg_ms = sum / all_latencies.size();
    }

    print_report(result, concurrency, is_streaming);
    return 0;
}
