#include "http_parser.hpp"
#include "spsc_queue.hpp"
#include "connection_manager.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <thread>
#include <chrono>

using namespace aigw;

void test_malformed_syntax() {
    std::cout << "[Corner Case 1] Malformed HTTP Request Syntax...\n";
    ParsedRequest req;

    // Completely empty string
    assert(!HttpParser::parse("", req));

    // Garbage non-HTTP line
    assert(!HttpParser::parse("JUNKDATA_WITHOUT_SPACES\r\n\r\n", req));

    // Missing header end delimiter \r\n\r\n
    assert(!HttpParser::parse("POST /v1/chat/completions HTTP/1.1\r\nHost: localhost", req));

    std::cout << "  -> PASSED: Malformed frames correctly rejected.\n";
}

void test_oversized_payload_protection() {
    std::cout << "[Corner Case 2] OOM Protection (Oversized Payload / Header)...\n";
    ConnectionManager mgr;
    int fake_fd = 42;
    std::vector<std::string> ready;

    // Test 1: Header exceeding 64KB limit
    std::string giant_header(ConnectionManager::MAX_HEADER_SIZE + 1024, 'A');
    FrameStatus status = mgr.process_incoming_data(fake_fd, giant_header.data(), giant_header.size(), ready);
    assert(status == FrameStatus::ERR_HEADER_TOO_LARGE);
    mgr.remove_connection(fake_fd);

    // Test 2: Content-Length claiming 100MB (exceeding 32MB limit)
    fake_fd = 43;
    std::string exploit_header = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 104857600\r\n\r\n";
    status = mgr.process_incoming_data(fake_fd, exploit_header.data(), exploit_header.size(), ready);
    assert(status == FrameStatus::ERR_PAYLOAD_TOO_LARGE);
    mgr.remove_connection(fake_fd);

    std::cout << "  -> PASSED: DoS/OOM payload attacks intercepted and blocked.\n";
}

void test_fragmented_tcp_reassembly() {
    std::cout << "[Corner Case 3] Fragmented TCP Edge-Triggered Reassembly...\n";
    ConnectionManager mgr;
    int fake_fd = 100;
    std::vector<std::string> ready;

    std::string part1 = "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\n";
    std::string part2 = "Content-Length: 25\r\n\r\n";
    std::string part3 = "{\"prompt\":";
    std::string part4 = " \"Hello LLM!\"}";

    // Chunk 1
    FrameStatus s1 = mgr.process_incoming_data(fake_fd, part1.data(), part1.size(), ready);
    assert(s1 == FrameStatus::NEED_MORE_DATA);
    assert(ready.empty());

    // Chunk 2 (Headers finish, but body incomplete)
    FrameStatus s2 = mgr.process_incoming_data(fake_fd, part2.data(), part2.size(), ready);
    assert(s2 == FrameStatus::NEED_MORE_DATA);
    assert(ready.empty());

    // Chunk 3 (Partial body)
    FrameStatus s3 = mgr.process_incoming_data(fake_fd, part3.data(), part3.size(), ready);
    assert(s3 == FrameStatus::NEED_MORE_DATA);
    assert(ready.empty());

    // Chunk 4 (Final body bytes)
    FrameStatus s4 = mgr.process_incoming_data(fake_fd, part4.data(), part4.size(), ready);
    assert(s4 == FrameStatus::OK);
    assert(ready.size() == 1);

    // Verify extracted request integrity
    ParsedRequest parsed;
    assert(HttpParser::parse(ready[0], parsed));
    assert(parsed.path == "/v1/chat/completions");
    assert(parsed.body == "{\"prompt\": \"Hello LLM!\"}");

    std::cout << "  -> PASSED: Fragmented frames reassembled across 4 epoll cycles.\n";
}

void test_queue_saturation_and_backpressure() {
    std::cout << "[Corner Case 4] SPSC Ring Buffer Saturation & Recovery...\n";
    const size_t CAP = 16;
    SpscQueue<int> q(CAP);

    // Fill completely to capacity
    for (size_t i = 0; i < CAP; ++i) {
        assert(q.try_push(static_cast<int>(i)));
    }

    // Must cleanly reject overflow without blocking or allocating
    assert(!q.try_push(9999));
    assert(!q.try_push(8888));

    // Drain half
    for (size_t i = 0; i < CAP / 2; ++i) {
        int val = -1;
        assert(q.try_pop(val));
        assert(val == static_cast<int>(i));
    }

    // Now push should succeed again immediately
    assert(q.try_push(100));
    assert(q.try_push(101));

    std::cout << "  -> PASSED: Wait-free backpressure maintained, queue recovered.\n";
}

void test_streaming_flag_detection() {
    std::cout << "[Corner Case 5] OpenAI Streaming Token Detection Edge Cases...\n";
    ParsedRequest req;

    std::string req_stream_true = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 32\r\n\r\n{\"stream\": true, \"model\":\"test\"}";
    assert(HttpParser::parse(req_stream_true, req));
    assert(req.is_streaming == true);

    std::string req_stream_nospace = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 31\r\n\r\n{\"stream\":true, \"model\":\"test\"}";
    assert(HttpParser::parse(req_stream_nospace, req));
    assert(req.is_streaming == true);

    std::string req_stream_false = "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 33\r\n\r\n{\"stream\": false, \"model\":\"test\"}";
    assert(HttpParser::parse(req_stream_false, req));
    assert(req.is_streaming == false);

    std::cout << "  -> PASSED: Fast zero-copy streaming detector correctly identified modes.\n";
}

int main() {
    std::cout << "=======================================================\n";
    std::cout << "  C++ Ingress Gateway Negative Scenarios & Corner Cases\n";
    std::cout << "=======================================================\n\n";

    test_malformed_syntax();
    test_oversized_payload_protection();
    test_fragmented_tcp_reassembly();
    test_queue_saturation_and_backpressure();
    test_streaming_flag_detection();

    std::cout << "\n=======================================================\n";
    std::cout << " ALL CORNER CASE & DEFENSIVE TESTS PASSED SUCCESSFULLY \n";
    std::cout << "=======================================================\n";
    return 0;
}
