#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

static std::atomic<bool> g_running{true};

void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_running.store(false);
    }
}

bool write_all(int fd, const char* data, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = write(fd, data + total, len - total);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

void handle_client(int client_fd) {
    char buf[8192];
    std::string request_data;
    
    // Read headers and body
    size_t expected_len = 0;
    bool headers_read = false;

    while (true) {
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n <= 0) break;
        request_data.append(buf, n);

        if (!headers_read) {
            size_t header_end = request_data.find("\r\n\r\n");
            if (header_end != std::string::npos) {
                headers_read = true;
                // Parse Content-Length
                size_t cl_pos = request_data.find("Content-Length:");
                if (cl_pos == std::string::npos) {
                    cl_pos = request_data.find("content-length:");
                }
                if (cl_pos != std::string::npos) {
                    size_t val_start = cl_pos + 15;
                    size_t val_end = request_data.find("\r\n", val_start);
                    if (val_end != std::string::npos) {
                        expected_len = header_end + 4 + std::stoull(request_data.substr(val_start, val_end - val_start));
                    }
                } else {
                    expected_len = header_end + 4;
                }
            }
        }

        if (headers_read && request_data.size() >= expected_len) {
            break;
        }
    }

    if (request_data.empty()) {
        close(client_fd);
        return;
    }

    std::string_view req(request_data);
    
    // 1. Models endpoint
    if (req.starts_with("GET /v1/models")) {
        std::string body = "{\"object\":\"list\",\"data\":[{\"id\":\"meta-llama/Llama-3-8B-Instruct\",\"object\":\"model\",\"owned_by\":\"vllm\"}]}";
        std::string resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
                           std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
        write_all(client_fd, resp.data(), resp.size());
        close(client_fd);
        return;
    }

    // 2. Chat Completions
    if (req.starts_with("POST /v1/chat/completions")) {
        bool is_streaming = (req.find("\"stream\": true") != std::string_view::npos ||
                             req.find("\"stream\":true") != std::string_view::npos);

        if (is_streaming) {
            // Send chunked SSE header
            std::string header = "HTTP/1.1 200 OK\r\n"
                                 "Content-Type: text/event-stream\r\n"
                                 "Cache-Control: no-cache\r\n"
                                 "Connection: close\r\n"
                                 "Access-Control-Allow-Origin: *\r\n\r\n";
            write_all(client_fd, header.data(), header.size());

            const std::vector<std::string> tokens = {
                "Inference", " streamed", " directly", " from", " pure", " C++",
                " AI", " backend", " through", " lock-free", " SPSC", " ring", " buffer!"
            };

            for (const auto& tok : tokens) {
                std::string chunk = "data: {\"id\":\"chatcmpl-cpp\",\"object\":\"chat.completion.chunk\","
                                    "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"" + tok + "\"}}]}\n\n";
                if (!write_all(client_fd, chunk.data(), chunk.size())) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(4)); // Simulated token generation
            }

            std::string done = "data: [DONE]\n\n";
            write_all(client_fd, done.data(), done.size());
        } else {
            std::string body = "{\"id\":\"chatcmpl-cpp-sync\",\"object\":\"chat.completion\","
                               "\"model\":\"meta-llama/Llama-3-8B-Instruct\","
                               "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\","
                               "\"content\":\"Echo from 100% C++ Mock AI Engine: Request received and processed with zero-copy!\"},"
                               "\"finish_reason\":\"stop\"}],"
                               "\"usage\":{\"prompt_tokens\":16,\"completion_tokens\":24,\"total_tokens\":40}}";
            std::string resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
                               std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
            write_all(client_fd, resp.data(), resp.size());
        }
    } else {
        std::string not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        write_all(client_fd, not_found.data(), not_found.size());
    }

    close(client_fd);
}

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int port = 8000;
    std::string unix_path = "";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-p" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "-s" && i + 1 < argc) {
            unix_path = argv[++i];
        }
    }

    int server_fd = -1;

    if (!unix_path.empty()) {
        unlink(unix_path.c_str());
        server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (server_fd < 0) {
            std::cerr << "[Mock-vLLM] Failed to create unix socket\n";
            return 1;
        }
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, unix_path.c_str(), sizeof(addr.sun_path) - 1);
        if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[Mock-vLLM] Failed to bind unix socket\n";
            return 1;
        }
        std::cout << "[Mock-vLLM] Listening on unix://" << unix_path << "\n";
    } else {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) {
            std::cerr << "[Mock-vLLM] Failed to create TCP socket\n";
            return 1;
        }
        int opt = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[Mock-vLLM] Failed to bind TCP port " << port << "\n";
            return 1;
        }
        std::cout << "[Mock-vLLM] Listening on http://0.0.0.0:" << port << " (C++ Mock AI Engine)\n";
    }

    listen(server_fd, 512);

    while (g_running.load()) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (!g_running.load()) break;
            continue;
        }

        // Spawn detached worker thread for high concurrency
        std::thread(handle_client, client_fd).detach();
    }

    if (server_fd >= 0) close(server_fd);
    if (!unix_path.empty()) unlink(unix_path.c_str());

    std::cout << "\n[Mock-vLLM] Shut down cleanly.\n";
    return 0;
}
