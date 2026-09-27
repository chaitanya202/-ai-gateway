CXX ?= g++
CXXFLAGS ?= -std=c++20 -O3 -march=native -Wall -Wextra -pthread -Iinclude

TARGET = ai_gateway
MOCK_TARGET = mock_vllm
STRESS_TARGET = ai_stress
TEST_TARGET = spsc_test
NEG_TEST_TARGET = negative_tests
CAP_BENCH_TARGET = capacity_benchmark
FAIL_TEST_TARGET = failure_cases_test

GATEWAY_SRCS = src/main.cpp src/epoll_reactor.cpp src/worker_pool.cpp src/backend_bridge.cpp
GATEWAY_OBJS = $(GATEWAY_SRCS:.cpp=.o)

all: $(TARGET) $(MOCK_TARGET) $(STRESS_TARGET) $(TEST_TARGET) $(NEG_TEST_TARGET) $(CAP_BENCH_TARGET) $(FAIL_TEST_TARGET)

$(TARGET): $(GATEWAY_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(MOCK_TARGET): src/mock_vllm.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

$(STRESS_TARGET): src/ai_stress.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

$(TEST_TARGET): test/spsc_test.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

$(NEG_TEST_TARGET): test/negative_tests.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

$(CAP_BENCH_TARGET): test/capacity_benchmark.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

$(FAIL_TEST_TARGET): test/failure_cases_test.cpp src/worker_pool.cpp src/backend_bridge.cpp
	$(CXX) $(CXXFLAGS) -o $@ $^

src/%.o: src/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(MOCK_TARGET) $(STRESS_TARGET) $(TEST_TARGET) $(NEG_TEST_TARGET) $(CAP_BENCH_TARGET) $(FAIL_TEST_TARGET) src/*.o

.PHONY: all clean
