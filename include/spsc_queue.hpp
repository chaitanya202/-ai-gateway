#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>
#include <type_traits>

namespace aigw {

// Cache line size for modern x86_64 architectures
constexpr size_t CACHE_LINE_SIZE = 64;

/**
 * Lock-free, wait-free Single-Producer Single-Consumer (SPSC) bounded ring buffer.
 * Optimized with:
 * - Cache-line padding to prevent false sharing between producer and consumer cores.
 * - Cached local indices to minimize atomic cross-core cache invalidation traffic.
 * - Power-of-2 capacity for bitwise mask indexing.
 * - Acquire-Release memory order semantics.
 */
template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity) {
        // Round up capacity to next power of 2
        capacity_ = round_up_to_power_of_two(capacity < 2 ? 2 : capacity);
        mask_ = capacity_ - 1;

        // Allocate raw uninitialized memory for slots
        buffer_ = static_cast<Slot*>(::operator new[](sizeof(Slot) * capacity_, std::align_val_t{CACHE_LINE_SIZE}));
        for (size_t i = 0; i < capacity_; ++i) {
            new (&buffer_[i]) Slot();
        }

        write_idx_.store(0, std::memory_order_relaxed);
        read_idx_.store(0, std::memory_order_relaxed);
        cached_read_idx_ = 0;
        cached_write_idx_ = 0;
    }

    ~SpscQueue() {
        // Destroy any remaining elements
        T item;
        while (try_pop(item)) {
            // drain
        }
        for (size_t i = 0; i < capacity_; ++i) {
            buffer_[i].~Slot();
        }
        ::operator delete[](buffer_, std::align_val_t{CACHE_LINE_SIZE});
    }

    // Non-copyable and non-movable
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    /**
     * Producer: Push an item into the queue.
     * Returns true if successful, false if queue is full.
     * Never blocks.
     */
    template <typename... Args>
    bool emplace(Args&&... args) {
        const size_t current_write = write_idx_.load(std::memory_order_relaxed);

        // Check if queue might be full using thread-local cached read index first
        if (current_write - cached_read_idx_ >= capacity_) {
            // Cache exhausted, refresh from consumer's atomic read index
            cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
            if (current_write - cached_read_idx_ >= capacity_) {
                return false; // Queue is full
            }
        }

        // Write directly to slot
        Slot& slot = buffer_[current_write & mask_];
        slot.construct(std::forward<Args>(args)...);

        // Publish new write index to consumer
        write_idx_.store(current_write + 1, std::memory_order_release);
        return true;
    }

    bool try_push(const T& item) {
        return emplace(item);
    }

    bool try_push(T&& item) {
        return emplace(std::move(item));
    }

    /**
     * Consumer: Pop an item from the queue.
     * Returns true if an item was retrieved, false if queue is empty.
     * Never blocks.
     */
    bool try_pop(T& item) {
        const size_t current_read = read_idx_.load(std::memory_order_relaxed);

        // Check if queue might be empty using thread-local cached write index first
        if (current_read >= cached_write_idx_) {
            // Cache exhausted, refresh from producer's atomic write index
            cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
            if (current_read >= cached_write_idx_) {
                return false; // Queue is empty
            }
        }

        // Read from slot
        Slot& slot = buffer_[current_read & mask_];
        item = std::move(slot.value());
        slot.destroy();

        // Publish new read index to producer
        read_idx_.store(current_read + 1, std::memory_order_release);
        return true;
    }

    /**
     * Approximate size (not strictly synchronized across threads).
     */
    [[nodiscard]] size_t size() const noexcept {
        const size_t w = write_idx_.load(std::memory_order_relaxed);
        const size_t r = read_idx_.load(std::memory_order_relaxed);
        return (w >= r) ? (w - r) : 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        return size() == 0;
    }

    [[nodiscard]] size_t capacity() const noexcept {
        return capacity_;
    }

private:
    struct Slot {
        alignas(alignof(T)) unsigned char storage[sizeof(T)];

        template <typename... Args>
        void construct(Args&&... args) {
            new (storage) T(std::forward<Args>(args)...);
        }

        T& value() noexcept {
            return *reinterpret_cast<T*>(storage);
        }

        void destroy() noexcept {
            value().~T();
        }
    };

    static size_t round_up_to_power_of_two(size_t val) {
        if (val == 0) return 1;
        --val;
        val |= val >> 1;
        val |= val >> 2;
        val |= val >> 4;
        val |= val >> 8;
        val |= val >> 16;
        val |= val >> 32;
        return val + 1;
    }

    size_t capacity_;
    size_t mask_;
    Slot* buffer_{nullptr};

    // Producer state - separated by cache line padding to prevent false sharing
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> write_idx_{0};
    size_t cached_read_idx_{0};

    // Consumer state - separated by cache line padding
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> read_idx_{0};
    size_t cached_write_idx_{0};

    // Trailing padding to isolate from adjacent heap structures
    alignas(CACHE_LINE_SIZE) char trailing_padding_[CACHE_LINE_SIZE]{};
};

} // namespace aigw
