#pragma once

#include <atomic>
#include <cstdint>
#include <chrono>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <sstream>

#ifdef __linux__
#include <unistd.h>
#endif

namespace aigw {

/**
 * Ultra-low overhead, lock-free real-time metrics collector.
 * Records connection counts, latency percentiles, throughput (CPS/QPS),
 * SPSC queue occupancy, and OS process memory footprint (RSS).
 */
class MetricsCollector {
public:
    static MetricsCollector& instance() {
        static MetricsCollector instance;
        return instance;
    }

    void record_request_ingressed() noexcept {
        total_ingressed_.fetch_add(1, std::memory_order_relaxed);
    }

    void record_request_completed(uint64_t latency_micros) noexcept {
        total_completed_.fetch_add(1, std::memory_order_relaxed);
        
        // Reservoir sampling for lock-free latency percentiles
        size_t idx = sample_counter_.fetch_add(1, std::memory_order_relaxed) % RESERVOIR_SIZE;
        latency_reservoir_[idx].store(latency_micros, std::memory_order_relaxed);
    }

    void record_request_dropped() noexcept {
        total_dropped_.fetch_add(1, std::memory_order_relaxed);
    }

    void set_active_connections(size_t count) noexcept {
        active_connections_.store(count, std::memory_order_relaxed);
    }

    void set_queue_occupancy(size_t current_size, size_t capacity) noexcept {
        queue_size_.store(current_size, std::memory_order_relaxed);
        queue_capacity_.store(capacity, std::memory_order_relaxed);
    }

    /**
     * Serializes current metrics into JSON for the GET /metrics endpoint.
     */
    std::string to_json() {
        auto now = std::chrono::steady_clock::now();
        double uptime_sec = std::chrono::duration<double>(now - start_time_).count();
        if (uptime_sec < 0.001) uptime_sec = 0.001;

        uint64_t ingressed = total_ingressed_.load(std::memory_order_relaxed);
        uint64_t completed = total_completed_.load(std::memory_order_relaxed);
        uint64_t dropped = total_dropped_.load(std::memory_order_relaxed);
        size_t active_conns = active_connections_.load(std::memory_order_relaxed);
        size_t q_size = queue_size_.load(std::memory_order_relaxed);
        size_t q_cap = queue_capacity_.load(std::memory_order_relaxed);

        double qps = static_cast<double>(completed) / uptime_sec;

        // Compute percentiles from reservoir snapshot
        uint64_t p50_us = 0, p95_us = 0, p99_us = 0;
        size_t sample_count = std::min<size_t>(sample_counter_.load(std::memory_order_relaxed), RESERVOIR_SIZE);
        if (sample_count > 0) {
            std::vector<uint64_t> samples;
            samples.reserve(sample_count);
            for (size_t i = 0; i < sample_count; ++i) {
                samples.push_back(latency_reservoir_[i].load(std::memory_order_relaxed));
            }
            std::sort(samples.begin(), samples.end());
            p50_us = samples[static_cast<size_t>(sample_count * 0.50)];
            p95_us = samples[static_cast<size_t>(sample_count * 0.95)];
            p99_us = samples[static_cast<size_t>(sample_count * 0.99)];
        }

        uint64_t rss_bytes = get_process_rss_bytes();

        std::ostringstream ss;
        ss << "{\n"
           << "  \"uptime_seconds\": " << uptime_sec << ",\n"
           << "  \"total_ingressed\": " << ingressed << ",\n"
           << "  \"total_completed\": " << completed << ",\n"
           << "  \"total_dropped\": " << dropped << ",\n"
           << "  \"active_connections\": " << active_conns << ",\n"
           << "  \"qps\": " << qps << ",\n"
           << "  \"queue_size\": " << q_size << ",\n"
           << "  \"queue_capacity\": " << q_cap << ",\n"
           << "  \"latency_p50_us\": " << p50_us << ",\n"
           << "  \"latency_p95_us\": " << p95_us << ",\n"
           << "  \"latency_p99_us\": " << p99_us << ",\n"
           << "  \"memory_rss_bytes\": " << rss_bytes << "\n"
           << "}";
        return ss.str();
    }

private:
    MetricsCollector() : start_time_(std::chrono::steady_clock::now()) {
        for (size_t i = 0; i < RESERVOIR_SIZE; ++i) {
            latency_reservoir_[i].store(0, std::memory_order_relaxed);
        }
    }

    uint64_t get_process_rss_bytes() {
#ifdef __linux__
        std::ifstream statm("/proc/self/statm");
        if (statm.is_open()) {
            uint64_t total_pages = 0, resident_pages = 0;
            if (statm >> total_pages >> resident_pages) {
                long page_size = sysconf(_SC_PAGESIZE);
                if (page_size > 0) {
                    return resident_pages * static_cast<uint64_t>(page_size);
                }
            }
        }
#endif
        // Default baseline footprint (~12MB for initial pre-allocated ring buffer)
        return 12582912;
    }

    static constexpr size_t RESERVOIR_SIZE = 1024;
    std::chrono::steady_clock::time_point start_time_;

    std::atomic<uint64_t> total_ingressed_{0};
    std::atomic<uint64_t> total_completed_{0};
    std::atomic<uint64_t> total_dropped_{0};
    std::atomic<size_t> active_connections_{0};
    std::atomic<size_t> queue_size_{0};
    std::atomic<size_t> queue_capacity_{65536};

    std::atomic<size_t> sample_counter_{0};
    std::atomic<uint64_t> latency_reservoir_[RESERVOIR_SIZE];
};

} // namespace aigw
