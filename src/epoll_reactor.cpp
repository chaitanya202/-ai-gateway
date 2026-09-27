#include "epoll_reactor.hpp"
#include "http_parser.hpp"
#include "metrics.hpp"

#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <iostream>

namespace aigw {

EpollReactor::EpollReactor(const GatewayConfig& config, WorkerPool& pool)
    : config_(config), worker_pool_(pool) {}

EpollReactor::~EpollReactor() {
    stop();
    if (server_fd_ >= 0) ::close(server_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

void EpollReactor::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
}

bool EpollReactor::init() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "[EpollReactor] Failed to create server socket: " << strerror(errno) << "\n";
        return false;
    }

    // Reuse address and port for rapid restart
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    set_nonblocking(server_fd_);

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.listen_port);
    if (inet_pton(AF_INET, config_.listen_host.c_str(), &server_addr.sin_addr) <= 0) {
        server_addr.sin_addr.s_addr = INADDR_ANY;
    }

    if (bind(server_fd_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        std::cerr << "[EpollReactor] Failed to bind to port " << config_.listen_port << ": " << strerror(errno) << "\n";
        return false;
    }

    if (listen(server_fd_, SOMAXCONN) < 0) {
        std::cerr << "[EpollReactor] Failed to listen: " << strerror(errno) << "\n";
        return false;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        std::cerr << "[EpollReactor] Failed to create epoll instance: " << strerror(errno) << "\n";
        return false;
    }

    // Add server socket in Edge-Triggered mode
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) {
        std::cerr << "[EpollReactor] Failed to add server socket to epoll: " << strerror(errno) << "\n";
        return false;
    }

    std::cout << "[EpollReactor] Gateway listening on " << config_.listen_host << ":" << config_.listen_port
              << " [Edge-Triggered epoll]\n";
    return true;
}

void EpollReactor::run() {
    running_.store(true, std::memory_order_relaxed);
    epoll_event events[MAX_EVENTS];

    while (running_.load(std::memory_order_relaxed)) {
        // 100ms timeout to allow responsive graceful shutdown check
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        if (num_events < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[EpollReactor] epoll_wait error: " << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == server_fd_) {
                handle_accept();
            } else {
                if (ev & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    close_client(fd);
                } else if (ev & EPOLLIN) {
                    handle_client_read(fd);
                }
            }
        }

        // Periodic sweep for Slowloris / stalled idle connections (every 5 seconds)
        static auto last_sweep = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (now - last_sweep > std::chrono::seconds(5)) {
            last_sweep = now;
            auto stale_fds = conn_mgr_.get_stale_connections(std::chrono::seconds(30));
            for (int stale_fd : stale_fds) {
                std::string timeout_err = HttpParser::response_408_timeout();
                ::write(stale_fd, timeout_err.data(), timeout_err.size());
                close_client(stale_fd);
            }
        }
    }
}

void EpollReactor::stop() {
    running_.store(false, std::memory_order_relaxed);
}

void EpollReactor::handle_accept() {
    // In Edge-Triggered mode, loop accept() until EAGAIN to accept all queued connections
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        
#ifdef __linux__
        int client_fd = accept4(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr),
                                &client_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
        int client_fd = accept(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd >= 0) set_nonblocking(client_fd);
#endif

        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // All pending connections accepted
                break;
            }
            if (errno == EINTR) continue;
            std::cerr << "[EpollReactor] accept error: " << strerror(errno) << "\n";
            break;
        }

        conn_mgr_.add_connection(client_fd);

        // Add client socket in Edge-Triggered mode with hangup notifications
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            std::cerr << "[EpollReactor] Failed to register client fd " << client_fd << " in epoll\n";
            close_client(client_fd);
        }
    }
}

void EpollReactor::handle_client_read(int client_fd) {
    char buf[8192];
    std::vector<std::string> ready_requests;

    FrameStatus status = FrameStatus::OK;

    // Loop recv() until EAGAIN/EWOULDBLOCK to drain all socket bytes in Edge-Triggered mode
    while (true) {
        ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
        if (n > 0) {
            status = conn_mgr_.process_incoming_data(client_fd, buf, static_cast<size_t>(n), ready_requests);
            if (status != FrameStatus::OK && status != FrameStatus::NEED_MORE_DATA) {
                break;
            }
        } else if (n == 0) {
            // Client closed connection cleanly
            close_client(client_fd);
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Fully drained available kernel buffer
                break;
            }
            if (errno == EINTR) continue;
            // Unrecoverable socket error
            close_client(client_fd);
            return;
        }
    }

    // Handle security and protocol constraint violations
    if (status == FrameStatus::ERR_HEADER_TOO_LARGE) {
        std::string err = HttpParser::response_431_header_too_large();
        ::write(client_fd, err.data(), err.size());
        close_client(client_fd);
        return;
    } else if (status == FrameStatus::ERR_PAYLOAD_TOO_LARGE) {
        std::string err = HttpParser::response_413_payload_too_large();
        ::write(client_fd, err.data(), err.size());
        close_client(client_fd);
        return;
    } else if (status == FrameStatus::ERR_MALFORMED) {
        std::string err = HttpParser::response_400_bad_request("{\"error\": \"Malformed HTTP request syntax\"}");
        ::write(client_fd, err.data(), err.size());
        close_client(client_fd);
        return;
    }

    // Dispatch any fully assembled HTTP request frames
    for (auto& req_str : ready_requests) {
        MetricsCollector::instance().record_request_ingressed();

        // 1. Try fast-path bypass first (healthz, metrics, models, static web)
        if (worker_pool_.handle_fast_path(client_fd, req_str)) {
            // Handled immediately on ingress; socket closed by fast-path handler
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
            conn_mgr_.remove_connection(client_fd);
            MetricsCollector::instance().set_active_connections(conn_mgr_.active_connections());
            break;
        }

        // 2. Inference request -> Dispatch to partitioned worker pool
        HttpRequestTask task{
            .client_fd = client_fd,
            .request_id = ++request_counter_,
            .ingress_time = std::chrono::steady_clock::now(),
            .raw_payload = std::move(req_str)
        };

        // Deregister from epoll as worker thread now owns this client_fd
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
        conn_mgr_.remove_connection(client_fd);
        MetricsCollector::instance().set_active_connections(conn_mgr_.active_connections());

        // Partition by client_fd across N lock-free worker queues
        size_t worker_idx = static_cast<size_t>(client_fd) % worker_pool_.worker_count();

        // Hand off to wait-free SPSC ring buffer (sub-microsecond)
        if (!worker_pool_.push_task(worker_idx, std::move(task))) {
            MetricsCollector::instance().record_request_dropped();
            // Queue capacity reached: return fast 503 backpressure response
            std::string overloaded = HttpParser::response_503_overloaded();
            ::write(client_fd, overloaded.data(), overloaded.size());
            ::close(client_fd);
        }

        // Ownership transferred; do not process further pipelined frames on this cycle
        break;
    }
}

void EpollReactor::close_client(int client_fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    conn_mgr_.remove_connection(client_fd);
    MetricsCollector::instance().set_active_connections(conn_mgr_.active_connections());
    ::close(client_fd);
}

} // namespace aigw
