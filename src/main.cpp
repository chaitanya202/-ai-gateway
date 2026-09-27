#include "types.hpp"
#include "worker_pool.hpp"
#include "epoll_reactor.hpp"

#include <csignal>
#include <iostream>
#include <memory>
#include <string>

static std::atomic<bool> g_shutdown_requested{false};
static aigw::EpollReactor* g_reactor_ptr{nullptr};

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_shutdown_requested.store(true);
        if (g_reactor_ptr) {
            g_reactor_ptr->stop();
        }
    }
}

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  -p <port>         Ingress HTTP port (default: 8080)\n"
              << "  -h <host>         Ingress bind host (default: 0.0.0.0)\n"
              << "  -u <upstream_host> Upstream AI engine host (default: 127.0.0.1)\n"
              << "  -P <upstream_port> Upstream AI engine port (default: 8000)\n"
              << "  -s <socket_path>  Upstream Unix domain socket (e.g. /tmp/vllm.sock)\n"
              << "  -k <api_key>      Required Bearer API key (optional)\n"
              << "  -q <capacity>     SPSC ring buffer capacity per worker (default: 65536)\n"
              << "  -w <workers>      Number of partitioned worker threads (default: 8)\n"
              << "  --help            Show this help message\n";
}

int main(int argc, char* argv[]) {
    // Ignore SIGPIPE to prevent crash when writing to disconnected client
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    aigw::GatewayConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "-p" && i + 1 < argc) {
            config.listen_port = std::stoi(argv[++i]);
        } else if (arg == "-h" && i + 1 < argc) {
            config.listen_host = argv[++i];
        } else if (arg == "-u" && i + 1 < argc) {
            config.upstream_host = argv[++i];
        } else if (arg == "-P" && i + 1 < argc) {
            config.upstream_port = std::stoi(argv[++i]);
        } else if (arg == "-s" && i + 1 < argc) {
            config.upstream_socket_path = argv[++i];
            config.use_unix_socket = true;
        } else if (arg == "-k" && i + 1 < argc) {
            config.required_api_key = argv[++i];
        } else if (arg == "-q" && i + 1 < argc) {
            config.queue_capacity = static_cast<size_t>(std::stoull(argv[++i]));
        } else if (arg == "-w" && i + 1 < argc) {
            config.worker_count = static_cast<size_t>(std::stoull(argv[++i]));
        }
    }

    std::cout << "========================================================\n"
              << "  OpenAI Ingress Gateway (Partitioned Multi-SPSC Pool)  \n"
              << "========================================================\n"
              << " Ingress Listen: " << config.listen_host << ":" << config.listen_port << "\n"
              << " Upstream AI:    " << (config.use_unix_socket ? ("unix://" + config.upstream_socket_path) : (config.upstream_host + ":" + std::to_string(config.upstream_port))) << "\n"
              << " Workers / Queues: " << config.worker_count << " parallel threads\n"
              << " SPSC Capacity:  " << config.queue_capacity << " slots per worker\n"
              << " Fast-Path:      Enabled (/healthz, /metrics, /v1/models)\n"
              << " API Auth Check: " << (config.required_api_key.empty() ? "Disabled (open)" : "Enforced") << "\n"
              << "========================================================\n\n";

    // 1. Allocate and start partitioned multi-queue worker pool
    aigw::WorkerPool pool(config);
    pool.start();

    // 2. Initialize and run Edge-Triggered epoll reactor
    aigw::EpollReactor reactor(config, pool);
    g_reactor_ptr = &reactor;

    if (!reactor.init()) {
        std::cerr << "[Main] Failed to initialize epoll reactor.\n";
        pool.stop();
        return 1;
    }

    std::cout << "[Main] Reactor active. Draining Edge-Triggered socket buffers.\n";
    reactor.run();

    std::cout << "\n[Main] Shutdown signal received. Stopping worker pool and draining queues...\n";
    pool.stop();

    std::cout << "========================================================\n"
              << " Summary Statistics:\n"
              << "   Total Ingressed Requests: " << reactor.total_requests_ingressed() << "\n"
              << "   Total Processed Tasks:    " << pool.total_tasks_processed() << "\n"
              << "========================================================\n";

    return 0;
}
