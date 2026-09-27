#pragma once

#include "types.hpp"
#include <string_view>
#include <charconv>
#include <algorithm>
#include <cctype>

namespace aigw {

class HttpParser {
public:
    /**
     * Parses raw HTTP request data into ParsedRequest.
     * Operates zero-copy via string_view wherever possible.
     */
    static bool parse(std::string_view raw, ParsedRequest& out) {
        if (raw.empty()) return false;

        // 1. Locate end of request line
        size_t line_end = raw.find("\r\n");
        if (line_end == std::string_view::npos) return false;

        std::string_view request_line = raw.substr(0, line_end);
        
        // Parse Method
        size_t first_space = request_line.find(' ');
        if (first_space == std::string_view::npos) return false;
        std::string_view method_str = request_line.substr(0, first_space);

        if (method_str == "GET") out.method = HttpMethod::GET;
        else if (method_str == "POST") out.method = HttpMethod::POST;
        else if (method_str == "PUT") out.method = HttpMethod::PUT;
        else if (method_str == "DELETE") out.method = HttpMethod::DELETE_;
        else if (method_str == "OPTIONS") out.method = HttpMethod::OPTIONS;
        else out.method = HttpMethod::UNKNOWN;

        // Parse URI
        size_t second_space = request_line.find(' ', first_space + 1);
        if (second_space == std::string_view::npos) return false;
        out.path = std::string(request_line.substr(first_space + 1, second_space - first_space - 1));
        out.http_version = std::string(request_line.substr(second_space + 1));

        // 2. Parse Headers
        size_t header_start = line_end + 2;
        size_t body_start = raw.find("\r\n\r\n", header_start);
        if (body_start == std::string_view::npos) return false;

        std::string_view headers_block = raw.substr(header_start, body_start - header_start);
        parse_headers(headers_block, out);

        // 3. Extract Body
        size_t actual_body_offset = body_start + 4;
        if (actual_body_offset <= raw.size()) {
            out.body = raw.substr(actual_body_offset);
        } else {
            out.body = std::string_view{};
        }

        // Fast scan for "stream": true in body for OpenAI requests
        out.is_streaming = detect_streaming(out.body);

        return true;
    }

    /**
     * Finds the boundary between HTTP headers and body (\r\n\r\n).
     * Returns std::string_view::npos if not found.
     */
    static size_t find_header_end(std::string_view raw) {
        return raw.find("\r\n\r\n");
    }

    /**
     * Parses Content-Length from header block string_view.
     * Returns 0 if not found or invalid.
     */
    static size_t extract_content_length(std::string_view headers_block) {
        // Case-insensitive search for Content-Length:
        size_t pos = 0;
        while (pos < headers_block.size()) {
            size_t next_line = headers_block.find("\r\n", pos);
            std::string_view line = (next_line == std::string_view::npos) 
                ? headers_block.substr(pos) 
                : headers_block.substr(pos, next_line - pos);

            size_t colon = line.find(':');
            if (colon != std::string_view::npos) {
                std::string_view name = trim(line.substr(0, colon));
                if (iequals(name, "content-length")) {
                    std::string_view val = trim(line.substr(colon + 1));
                    size_t length = 0;
                    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), length);
                    if (ec == std::errc()) {
                        return length;
                    }
                }
            }

            if (next_line == std::string_view::npos) break;
            pos = next_line + 2;
        }
        return 0;
    }

    // Fast static HTTP response builders
    static std::string build_response(int status_code, std::string_view status_text, 
                                      std::string_view content_type, std::string_view body,
                                      bool keep_alive = true) {
        std::string res;
        res.reserve(256 + body.size());
        res.append("HTTP/1.1 ").append(std::to_string(status_code)).append(" ").append(status_text).append("\r\n");
        res.append("Server: aigw-ingress/1.0\r\n");
        res.append("Content-Type: ").append(content_type).append("\r\n");
        res.append("Content-Length: ").append(std::to_string(body.size())).append("\r\n");
        res.append("Connection: ").append(keep_alive ? "keep-alive" : "close").append("\r\n");
        res.append("Access-Control-Allow-Origin: *\r\n");
        res.append("Access-Control-Allow-Headers: Authorization, Content-Type\r\n");
        res.append("\r\n");
        res.append(body);
        return res;
    }

    static std::string response_200_json(std::string_view body) {
        return build_response(200, "OK", "application/json", body);
    }

    static std::string response_400_bad_request(std::string_view msg = "{\"error\": \"Bad Request\"}") {
        return build_response(400, "Bad Request", "application/json", msg, false);
    }

    static std::string response_401_unauthorized(std::string_view msg = "{\"error\": \"Invalid API Key\"}") {
        return build_response(401, "Unauthorized", "application/json", msg, false);
    }

    static std::string response_404_not_found(std::string_view msg = "{\"error\": \"Endpoint Not Found\"}") {
        return build_response(404, "Not Found", "application/json", msg, false);
    }

    static std::string response_408_timeout(std::string_view msg = "{\"error\": \"Request Timeout\"}") {
        return build_response(408, "Request Timeout", "application/json", msg, false);
    }

    static std::string response_413_payload_too_large(std::string_view msg = "{\"error\": \"Payload Too Large (Exceeds 32MB limit)\"}") {
        return build_response(413, "Payload Too Large", "application/json", msg, false);
    }

    static std::string response_431_header_too_large(std::string_view msg = "{\"error\": \"Request Header Fields Too Large (Exceeds 64KB limit)\"}") {
        return build_response(431, "Request Header Fields Too Large", "application/json", msg, false);
    }

    static std::string response_502_bad_gateway(std::string_view msg = "{\"error\": \"Bad Gateway (Upstream AI Engine Unreachable)\"}") {
        return build_response(502, "Bad Gateway", "application/json", msg, false);
    }

    static std::string response_503_overloaded(std::string_view msg = "{\"error\": \"Queue Full / Service Overloaded\"}") {
        return build_response(503, "Service Unavailable", "application/json", msg, false);
    }

    static std::string response_504_gateway_timeout(std::string_view msg = "{\"error\": \"Gateway Timeout (Upstream Inference Timed Out)\"}") {
        return build_response(504, "Gateway Timeout", "application/json", msg, false);
    }

private:
    static void parse_headers(std::string_view headers_block, ParsedRequest& out) {
        size_t pos = 0;
        while (pos < headers_block.size()) {
            size_t next_line = headers_block.find("\r\n", pos);
            std::string_view line = (next_line == std::string_view::npos) 
                ? headers_block.substr(pos) 
                : headers_block.substr(pos, next_line - pos);

            size_t colon = line.find(':');
            if (colon != std::string_view::npos) {
                std::string_view key = trim(line.substr(0, colon));
                std::string_view val = trim(line.substr(colon + 1));

                if (iequals(key, "authorization")) {
                    if (val.starts_with("Bearer ") || val.starts_with("bearer ")) {
                        out.api_key = std::string(val.substr(7));
                    } else {
                        out.api_key = std::string(val);
                    }
                } else if (iequals(key, "content-type")) {
                    out.content_type = std::string(val);
                } else if (iequals(key, "content-length")) {
                    size_t len = 0;
                    auto [ptr, ec] = std::from_chars(val.data(), val.data() + val.size(), len);
                    if (ec == std::errc()) {
                        out.content_length = len;
                    }
                }
            }

            if (next_line == std::string_view::npos) break;
            pos = next_line + 2;
        }
    }

    static bool detect_streaming(std::string_view body) {
        // Fast search for "stream": true or "stream":true
        size_t pos = body.find("\"stream\"");
        if (pos == std::string_view::npos) return false;
        
        size_t colon = body.find(':', pos + 8);
        if (colon == std::string_view::npos) return false;
        
        // Scan next characters for 'true'
        size_t val_pos = colon + 1;
        while (val_pos < body.size() && (body[val_pos] == ' ' || body[val_pos] == '\t')) {
            ++val_pos;
        }
        return body.substr(val_pos).starts_with("true");
    }

    static std::string_view trim(std::string_view s) {
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
        while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
        return s;
    }

    static bool iequals(std::string_view a, std::string_view b) {
        if (a.size() != b.size()) return false;
        for (size_t i = 0; i < a.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i]))) {
                return false;
            }
        }
        return true;
    }
};

} // namespace aigw
