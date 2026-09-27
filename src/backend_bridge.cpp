#include "backend_bridge.hpp"
#include "http_parser.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <iostream>

namespace aigw {

BackendBridge::BackendBridge(const GatewayConfig& config)
    : config_(config) {}

BackendBridge::~BackendBridge() = default;

int BackendBridge::connect_to_backend() {
    int fd = -1;

    if (config_.use_unix_socket) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, config_.upstream_socket_path.c_str(), sizeof(addr.sun_path) - 1);

        if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }
    } else {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(config_.upstream_port);
        if (inet_pton(AF_INET, config_.upstream_host.c_str(), &addr.sin_addr) <= 0) {
            close(fd);
            return -1;
        }

        if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }
    }

    // Set 30-second inference timeout on upstream socket
    struct timeval tv;
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));

    return fd;
}

bool BackendBridge::write_all(int fd, const char* data, size_t length) {
    size_t total_sent = 0;
    while (total_sent < length) {
        ssize_t sent = write(fd, data + total_sent, length - total_sent);
        if (sent <= 0) {
            return false;
        }
        total_sent += sent;
    }
    return true;
}

bool BackendBridge::forward_and_relay(int client_fd, const std::string& raw_request, bool is_streaming) {
    (void)is_streaming;
    int backend_fd = connect_to_backend();
    if (backend_fd < 0) {
        std::string err_resp = HttpParser::response_502_bad_gateway(
            "{\"error\": {\"message\": \"Upstream AI engine is unreachable at " +
            (config_.use_unix_socket ? config_.upstream_socket_path : (config_.upstream_host + ":" + std::to_string(config_.upstream_port))) +
            "\", \"type\": \"bad_gateway_error\"}}");
        write_all(client_fd, err_resp.data(), err_resp.size());
        close(client_fd);
        return false;
    }

    // 1. Send the HTTP request payload directly to local AI backend
    if (!write_all(backend_fd, raw_request.data(), raw_request.size())) {
        close(backend_fd);
        close(client_fd);
        return false;
    }

    // 2. Stream/relay the response back to client_fd in real time
    char buffer[16384];
    bool bytes_relayed = false;

    while (true) {
        ssize_t bytes_read = read(backend_fd, buffer, sizeof(buffer));
        if (bytes_read > 0) {
            bytes_relayed = true;
            if (!write_all(client_fd, buffer, static_cast<size_t>(bytes_read))) {
                // Client aborted connection mid-stream (e.g. browser closed tab).
                // Terminate upstream read immediately to save GPU inference cycles.
                break;
            }
        } else if (bytes_read == 0) {
            // Backend finished sending response (EOF)
            break;
        } else {
            // Read error or upstream timeout
            if (!bytes_relayed && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::string timeout_resp = HttpParser::response_504_gateway_timeout(
                    "{\"error\": {\"message\": \"Upstream AI inference engine timed out\", \"type\": \"timeout_error\"}}");
                write_all(client_fd, timeout_resp.data(), timeout_resp.size());
            }
            break;
        }
    }

    close(backend_fd);
    close(client_fd);
    return true;
}

} // namespace aigw
