#pragma once

#include "types.hpp"
#include "worker_pool.hpp"
#include "connection_manager.hpp"

#include <atomic>
#include <vector>

namespace aigw {

/**
 * Edge-Triggered epoll network reactor.
 * Responsibilities:
 * - Accept TCP connections non-blockingly.
 * - Drain socket buffers completely on EPOLLIN using EPOLLET until EAGAIN.
 * - Accumulate HTTP frames via ConnectionManager.
 * - Routes through fast-path bypass for health/metrics/models, or hashes into partitioned worker pool.
 * - Absolutely zero JSON parsing or blocking I/O on this thread.
 */
class EpollReactor {
public:
    static constexpr int MAX_EVENTS = 1024;

    EpollReactor(const GatewayConfig& config, WorkerPool& pool);
    ~EpollReactor();

    // Non-copyable
    EpollReactor(const EpollReactor&) = delete;
    EpollReactor& operator=(const EpollReactor&) = delete;

    bool init();
    void run();
    void stop();

    uint64_t total_requests_ingressed() const noexcept {
        return request_counter_.load(std::memory_order_relaxed);
    }

private:
    void handle_accept();
    void handle_client_read(int client_fd);
    void close_client(int client_fd);
    void set_nonblocking(int fd);

    GatewayConfig config_;
    WorkerPool& worker_pool_;
    ConnectionManager conn_mgr_;

    int server_fd_{-1};
    int epoll_fd_{-1};
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> request_counter_{0};
};

} // namespace aigw
