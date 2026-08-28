# The matcher's engine core, header-only on purpose: the whole hot path lives in templates the
# process binary instantiates once, so claim, commit and every book operation inline with no
# dispatched call anywhere in the match loop (P-13).

add_library(exchange-matcher INTERFACE)
target_include_directories(exchange-matcher INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-matcher INTERFACE exchange-protocol exchange-common)

add_library(matcher-test-harness STATIC test/harness.cpp)
target_include_directories(matcher-test-harness PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(matcher-test-harness PUBLIC exchange-matcher)

add_executable(matcher-corpus test/corpus_test.cpp)
target_link_libraries(matcher-corpus PRIVATE matcher-test-harness Catch2::Catch2WithMain)

add_executable(matcher-invariants test/invariants_test.cpp)
target_link_libraries(matcher-invariants PRIVATE matcher-test-harness Catch2::Catch2WithMain)

add_executable(matcher-attribution test/attribution_test.cpp)
target_link_libraries(matcher-attribution PRIVATE matcher-test-harness Catch2::Catch2WithMain)

# The process itself: rings in and out, a journal, snapshots, and replay as the recovery path.
add_executable(matcher src/matcher_main.cpp)
target_link_libraries(matcher PRIVATE exchange-matcher)
exchange_measured(matcher)

add_executable(matcher-determinism test/determinism_test.cpp)
target_link_libraries(matcher-determinism PRIVATE matcher-test-harness Catch2::Catch2WithMain)
target_compile_definitions(matcher-determinism
  PRIVATE MATCHER_BINARY="$<TARGET_FILE:matcher>")
add_dependencies(matcher-determinism matcher)

add_executable(matcher-ring test/ring_test.cpp)
target_link_libraries(matcher-ring PRIVATE exchange-matcher Catch2::Catch2WithMain)

add_executable(matcher-broadcast test/broadcast_test.cpp)
target_link_libraries(matcher-broadcast PRIVATE exchange-matcher Catch2::Catch2WithMain)

add_executable(matcher-shard test/shard_test.cpp)
target_link_libraries(matcher-shard PRIVATE matcher-test-harness Catch2::Catch2WithMain)

# The proofs that run as binaries: the allocation probe whose allocator counts after
# initialisation, the flow generator that writes the shared journal, and the measurement harness.
add_executable(allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(allocation-probe PRIVATE matcher-test-harness)

add_executable(matcher-allocation test/allocation_probe_test.cpp)
target_link_libraries(matcher-allocation PRIVATE matcher-test-harness Catch2::Catch2WithMain)
target_compile_definitions(matcher-allocation
  PRIVATE ALLOCATION_PROBE_BINARY="$<TARGET_FILE:allocation-probe>")
add_dependencies(matcher-allocation allocation-probe)

add_executable(flowgen test/flowgen_main.cpp)
target_link_libraries(flowgen PRIVATE matcher-test-harness)

add_executable(matcher-benchmark src/benchmark_main.cpp)
target_link_libraries(matcher-benchmark PRIVATE exchange-matcher)
exchange_measured(matcher-benchmark)

catch_discover_tests(matcher-corpus)
catch_discover_tests(matcher-invariants)
catch_discover_tests(matcher-attribution)
catch_discover_tests(matcher-determinism)
catch_discover_tests(matcher-ring)
catch_discover_tests(matcher-broadcast)
catch_discover_tests(matcher-shard)
catch_discover_tests(matcher-allocation)
