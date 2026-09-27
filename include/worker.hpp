#pragma once

#include "types.hpp"
#include "spsc_queue.hpp"
#include "backend_bridge.hpp"

#include <thread>
#include <atomic>
#include <memory>

namespace aigw {

class WorkerThread {
public:
    WorkerThread(SpscQueue<HttpRequestTask>& queue, const GatewayConfig& config);
    ~WorkerThread();

    // Non-copyable
    WorkerThread(const WorkerThread&) = delete;
    WorkerThread& operator=(const WorkerThread&) = delete;

    void start();
    void stop();

    uint64_t total_requests_processed() const noexcept {
        return total_processed_.load(std::memory_order_relaxed);
    }

private:
    void run();
    void process_task(HttpRequestTask& task);

    SpscQueue<HttpRequestTask>& queue_;
    GatewayConfig config_;
    BackendBridge backend_bridge_;
    
    std::atomic<bool> running_{false};
    std::thread thread_;
    std::atomic<uint64_t> total_processed_{0};
};

} // namespace aigw
