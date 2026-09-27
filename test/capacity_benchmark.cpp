#include "spsc_queue.hpp"
#include "http_parser.hpp"
#include "connection_manager.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <numeric>
#include <algorithm>
#include <iomanip>

using namespace aigw;

void benchmark_spsc_max_capacity(size_t queue_size, uint64_t total_ops) {
    std::cout << "--- 1. Lock-Free SPSC Max Saturation Throughput ---\n";
    std::cout << "Queue Capacity: " << queue_size << " slots | Total Elements: " << total_ops << "\n";

    SpscQueue<uint64_t> queue(queue_size);

    auto start_time = std::chrono::high_resolution_clock::now();

    std::thread producer([&]() {
        for (uint64_t i = 0; i < total_ops; ++i) {
            while (!queue.try_push(i)) {
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#else
                std::this_thread::yield();
#endif
            }
        }
    });

    std::thread consumer([&]() {
        uint64_t received = 0;
        uint64_t val = 0;
        while (received < total_ops) {
            if (queue.try_pop(val)) {
                ++received;
            } else {
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#else
                std::this_thread::yield();
#endif
            }
        }
    });

    producer.join();
    consumer.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double ops_per_sec = static_cast<double>(total_ops) / elapsed_sec;
    double ns_per_op = (elapsed_sec / total_ops) * 1e9;

    std::cout << "  Elapsed Time:  " << std::fixed << std::setprecision(4) << elapsed_sec << " s\n";
    std::cout << "  Throughput:    " << std::setprecision(2) << (ops_per_sec / 1e6) << " Million ops/sec\n";
    std::cout << "  Latency:       " << std::setprecision(2) << ns_per_op << " ns per operation\n\n";
}

void benchmark_frame_parser_capacity(uint64_t total_frames) {
    std::cout << "--- 2. Zero-Copy HTTP Frame Assembler Capacity ---\n";
    std::string sample_req = 
        "POST /v1/chat/completions HTTP/1.1\r\n"
        "Host: 127.0.0.1:8080\r\n"
        "Authorization: Bearer sk-antigravity-prod-key\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 72\r\n"
        "\r\n"
        "{\"model\":\"meta-llama/Llama-3-8B\",\"messages\":[{\"role\":\"user\",\"content\":\"Hi\"}]}";

    auto start_time = std::chrono::high_resolution_clock::now();

    uint64_t parsed_count = 0;
    ParsedRequest req;
    for (uint64_t i = 0; i < total_frames; ++i) {
        if (HttpParser::parse(sample_req, req)) {
            ++parsed_count;
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double frames_per_sec = static_cast<double>(parsed_count) / elapsed_sec;

    std::cout << "  Total Frames Parsed: " << parsed_count << "\n";
    std::cout << "  Elapsed Time:        " << std::fixed << std::setprecision(4) << elapsed_sec << " s\n";
    std::cout << "  Throughput:          " << std::setprecision(2) << (frames_per_sec / 1e6) << " Million requests/sec\n";
    std::cout << "  Average Parse Time:  " << std::setprecision(2) << ((elapsed_sec / parsed_count) * 1e9) << " ns\n\n";
}

int main() {
    std::cout << "=================================================================\n";
    std::cout << "    AI INGRESS GATEWAY - MAXIMUM CAPACITY PROFILER               \n";
    std::cout << "=================================================================\n\n";

    benchmark_spsc_max_capacity(65536, 10'000'000);
    benchmark_frame_parser_capacity(5'000'000);

    std::cout << "=================================================================\n";
    std::cout << "  MAX CAPACITY PROFILING COMPLETE                                \n";
    std::cout << "=================================================================\n";
    return 0;
}
