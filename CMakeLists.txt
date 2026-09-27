cmake_minimum_required(VERSION 3.20)
project(ai_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization and performance flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -O3
        -march=native
        -Wall
        -Wextra
        -Wpedantic
        -pthread
    )
endif()

include_directories(include)

# 1. Main Ingress Gateway (Linux/WSL)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(ai_gateway
        src/main.cpp
        src/epoll_reactor.cpp
        src/worker_pool.cpp
        src/backend_bridge.cpp
    )
    target_link_libraries(ai_gateway PRIVATE pthread)

    # 2. Native C++ Mock AI Engine (vLLM simulation)
    add_executable(mock_vllm src/mock_vllm.cpp)
    target_link_libraries(mock_vllm PRIVATE pthread)

    # 3. Native C++ Multi-Threaded Load Tester
    add_executable(ai_stress src/ai_stress.cpp)
    target_link_libraries(ai_stress PRIVATE pthread)
endif()

# 4. Standalone unit test for lock-free SPSC queue (cross-platform)
add_executable(spsc_test test/spsc_test.cpp)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(spsc_test PRIVATE pthread)
endif()

# 5. Negative scenarios and corner cases test
add_executable(negative_tests test/negative_tests.cpp)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(negative_tests PRIVATE pthread)
endif()

# 6. Maximum capacity and saturation profiler
add_executable(capacity_benchmark test/capacity_benchmark.cpp)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(capacity_benchmark PRIVATE pthread)
endif()

# 7. Comprehensive resilience and failure cases suite
add_executable(failure_cases_test
    test/failure_cases_test.cpp
    src/worker_pool.cpp
    src/backend_bridge.cpp
)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_link_libraries(failure_cases_test PRIVATE pthread)
endif()
