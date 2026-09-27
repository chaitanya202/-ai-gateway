#include "spsc_queue.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <cassert>

using namespace aigw;

void test_basic() {
    std::cout << "[Test 1] Basic push, pop, and capacity...\n";
    SpscQueue<int> q(8);
    assert(q.capacity() == 8);
    assert(q.empty());

    for (int i = 0; i < 8; ++i) {
        bool ok = q.try_push(i);
        assert(ok);
    }
    // Now full
    assert(!q.try_push(999));

    for (int i = 0; i < 8; ++i) {
        int val = -1;
        bool ok = q.try_pop(val);
        assert(ok);
        assert(val == i);
    }
    // Now empty
    int dummy = -1;
    assert(!q.try_pop(dummy));
    std::cout << "  -> PASSED\n";
}

void test_concurrent_stress(size_t num_items) {
    std::cout << "[Test 2] High-throughput concurrent stress test (" << num_items << " elements)...\n";
    SpscQueue<uint64_t> q(65536);

    auto start_time = std::chrono::high_resolution_clock::now();

    // Producer thread
    std::thread producer([&]() {
        for (uint64_t i = 0; i < num_items; ++i) {
            while (!q.try_push(i)) {
                // Wait-free spin with CPU pause
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#else
                std::this_thread::yield();
#endif
            }
        }
    });

    // Consumer thread
    std::vector<uint64_t> errors;
    std::thread consumer([&]() {
        uint64_t expected = 0;
        uint64_t val = 0;
        while (expected < num_items) {
            if (q.try_pop(val)) {
                if (val != expected) {
                    errors.push_back(val);
                }
                ++expected;
            } else {
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#else
                std::this_thread::yield();
#endif
            }
        }
    });

    producer.join();
    consumer.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double ops_per_sec = static_cast<double>(num_items) / elapsed_sec;

    assert(errors.empty());
    std::cout << "  -> PASSED: Zero drops, zero out-of-order errors.\n";
    std::cout << "  -> Elapsed time: " << elapsed_sec << " s\n";
    std::cout << "  -> Throughput:   " << (ops_per_sec / 1e6) << " Million ops/sec\n";
}

int main() {
    std::cout << "========================================\n";
    std::cout << "  SPSC Lock-Free Ring Buffer Test Suite \n";
    std::cout << "========================================\n";

    test_basic();
    test_concurrent_stress(10'000'000);

    std::cout << "\nAll SPSC ring buffer tests PASSED successfully!\n";
    return 0;
}
