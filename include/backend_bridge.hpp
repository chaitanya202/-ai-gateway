#pragma once

#include "types.hpp"
#include <string>
#include <string_view>
#include <memory>

namespace aigw {

/**
 * Handles communication between the gateway worker and the local AI backend (Python/vLLM).
 * Supports both Unix Domain Sockets (zero-network IPC) and TCP Loopback.
 * Relays responses (including streaming Server-Sent Events) back to the client socket.
 */
class BackendBridge {
public:
    explicit BackendBridge(const GatewayConfig& config);
    ~BackendBridge();

    // Non-copyable
    BackendBridge(const BackendBridge&) = delete;
    BackendBridge& operator=(const BackendBridge&) = delete;

    /**
     * Forwards an assembled HTTP request to the local AI engine,
     * and streams or relays the response back to client_fd.
     * Returns true if forwarded successfully, false on IPC error.
     */
    bool forward_and_relay(int client_fd, const std::string& raw_request, bool is_streaming);

private:
    int connect_to_backend();
    bool write_all(int fd, const char* data, size_t length);

    GatewayConfig config_;
};

} // namespace aigw
