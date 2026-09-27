#pragma once

#include "http_parser.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <chrono>

namespace aigw {

enum class FrameStatus {
    OK,
    NEED_MORE_DATA,
    ERR_HEADER_TOO_LARGE,
    ERR_PAYLOAD_TOO_LARGE,
    ERR_MALFORMED
};

/**
 * Manages per-connection streaming buffers for the Edge-Triggered epoll reactor.
 * Protects against:
 * - Buffer overflow & OOM attacks (max header and body enforcement)
 * - Fragmented TCP packets across multiple epoll wakeups
 * - Pipelined requests over persistent keep-alive connections
 * - Slowloris / idle connection resource starvation
 */
class ConnectionManager {
public:
    static constexpr size_t MAX_HEADER_SIZE = 64 * 1024;        // 64 KB maximum header block
    static constexpr size_t MAX_BODY_SIZE = 32 * 1024 * 1024;   // 32 MB maximum JSON prompt payload

    struct Connection {
        int fd = -1;
        std::string buffer;
        bool headers_complete = false;
        size_t expected_total_bytes = 0;
        size_t header_end_pos = 0;
        std::chrono::steady_clock::time_point last_activity;
    };

    void add_connection(int fd) {
        connections_[fd] = Connection{
            .fd = fd,
            .buffer = "",
            .headers_complete = false,
            .expected_total_bytes = 0,
            .header_end_pos = 0,
            .last_activity = std::chrono::steady_clock::now()
        };
    }

    void remove_connection(int fd) {
        connections_.erase(fd);
    }

    bool has_connection(int fd) const {
        return connections_.find(fd) != connections_.end();
    }

    /**
     * Ingests freshly read socket data, parses frame boundaries, and extracts complete requests.
     * Returns FrameStatus indicating success or specific protocol violation.
     */
    FrameStatus process_incoming_data(int fd, const char* data, size_t len, std::vector<std::string>& ready_requests) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) {
            add_connection(fd);
            it = connections_.find(fd);
        }

        Connection& conn = it->second;
        conn.last_activity = std::chrono::steady_clock::now();

        // Guard against unbounded buffer allocation
        if (conn.buffer.size() + len > MAX_HEADER_SIZE + MAX_BODY_SIZE) {
            return FrameStatus::ERR_PAYLOAD_TOO_LARGE;
        }

        conn.buffer.append(data, len);

        while (!conn.buffer.empty()) {
            if (!conn.headers_complete) {
                size_t header_end = HttpParser::find_header_end(conn.buffer);
                if (header_end == std::string_view::npos) {
                    // Check if uncompleted header block exceeds safe memory limit
                    if (conn.buffer.size() > MAX_HEADER_SIZE) {
                        return FrameStatus::ERR_HEADER_TOO_LARGE;
                    }
                    return FrameStatus::NEED_MORE_DATA;
                }

                conn.header_end_pos = header_end + 4; // Include \r\n\r\n delimiter
                std::string_view headers_block(conn.buffer.data(), header_end);
                
                // Validate request line basic structure
                size_t first_crlf = headers_block.find("\r\n");
                if (first_crlf == std::string_view::npos) {
                    first_crlf = headers_block.size();
                }
                std::string_view req_line = headers_block.substr(0, first_crlf);
                if (req_line.find(' ') == std::string_view::npos) {
                    return FrameStatus::ERR_MALFORMED;
                }

                size_t content_length = HttpParser::extract_content_length(headers_block);
                if (content_length > MAX_BODY_SIZE) {
                    return FrameStatus::ERR_PAYLOAD_TOO_LARGE;
                }

                conn.expected_total_bytes = conn.header_end_pos + content_length;
                conn.headers_complete = true;
            }

            if (conn.headers_complete) {
                if (conn.buffer.size() >= conn.expected_total_bytes) {
                    // Fully framed HTTP request extracted
                    ready_requests.push_back(conn.buffer.substr(0, conn.expected_total_bytes));

                    // Shift buffer for pipelined requests
                    conn.buffer.erase(0, conn.expected_total_bytes);
                    conn.headers_complete = false;
                    conn.expected_total_bytes = 0;
                    conn.header_end_pos = 0;
                } else {
                    return FrameStatus::NEED_MORE_DATA;
                }
            }
        }

        return FrameStatus::OK;
    }

    /**
     * Identifies connections that have been idle past the specified timeout window.
     */
    std::vector<int> get_stale_connections(std::chrono::seconds timeout_duration) const {
        std::vector<int> stale;
        auto now = std::chrono::steady_clock::now();
        for (const auto& [fd, conn] : connections_) {
            if (now - conn.last_activity > timeout_duration) {
                stale.push_back(fd);
            }
        }
        return stale;
    }

    size_t active_connections() const noexcept {
        return connections_.size();
    }

private:
    std::unordered_map<int, Connection> connections_;
};

} // namespace aigw
