#pragma once

#include "types.hpp"
#include "spsc_queue.hpp"
#include "backend_bridge.hpp"

#include <vector>
#include <thread>
#include <atomic>
#include <memory>

namespace aigw {

/**
 * Manages a partitioned pool of independent, lock-free SPSC ring buffers and worker threads.
 * Eliminates the single-consumer bottleneck by scaling concurrent upstream streaming across
 * N CPU cores, and provides an immediate Fast-Path bypass for lightweight requests.
 */
class WorkerPool {
public:
    explicit WorkerPool(const GatewayConfig& config);
    ~WorkerPool();

    // Non-copyable
    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    void start();
    void stop();

    /**
     * Intercepts lightweight queries (/healthz, /metrics, /v1/models, web dashboard)
     * and responds immediately on the ingress path, bypassing inference queues.
     * Returns true if handled, false if request requires upstream LLM inference.
     */
    bool handle_fast_path(int client_fd, const std::string& raw_payload);

    /**
     * Pushes an assembled inference task into the specified worker's lock-free SPSC queue.
     * Returns true if enqueued, false if the worker's ring buffer is saturated.
     */
    bool push_task(size_t worker_idx, HttpRequestTask&& task);

    size_t worker_count() const noexcept {
        return worker_count_;
    }

    uint64_t total_tasks_processed() const noexcept {
        return total_processed_.load(std::memory_order_relaxed);
    }

    size_t total_queue_size() const noexcept;
    size_t total_queue_capacity() const noexcept;

private:
    void worker_loop(size_t worker_id);
    void process_inference_task(HttpRequestTask& task, BackendBridge& bridge);

    GatewayConfig config_;
    size_t worker_count_;
    std::vector<std::unique_ptr<SpscQueue<HttpRequestTask>>> queues_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> total_processed_{0};
};

} // namespace aigw
