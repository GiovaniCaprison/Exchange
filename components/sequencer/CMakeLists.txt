# The sequencer: where nondeterminism becomes a recorded fact. Header-only like the matcher, so
# arbitration, dedupe, stamping and publication inline into the process binary with no dispatched
# call on the path (P-13).

add_library(exchange-sequencer INTERFACE)
target_include_directories(exchange-sequencer INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-sequencer INTERFACE exchange-protocol exchange-common)

# The suites borrow the matcher's harness on purpose: its CommandWriter stamps sequence and
# timestamp exactly as the sequencer must, so byte equality against a hand-stamped flow is the
# core proof rather than a convenience.
add_executable(sequencer-stream test/stream_test.cpp)
target_link_libraries(sequencer-stream
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

add_executable(sequencer-dedupe test/dedupe_test.cpp)
target_link_libraries(sequencer-dedupe
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

add_executable(sequencer-packets test/packets_test.cpp)
target_link_libraries(sequencer-packets
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

add_executable(sequencer-replication test/replication_test.cpp)
target_link_libraries(sequencer-replication
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

add_executable(sequencer-chaos test/chaos_test.cpp)
target_link_libraries(sequencer-chaos
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

add_executable(sequencer-sources test/sources_test.cpp)
target_link_libraries(sequencer-sources
  PRIVATE exchange-sequencer matcher-test-harness Catch2::Catch2WithMain)

# The process itself, and the generator that writes the submission files the proofs share.
add_executable(sequencer src/sequencer_main.cpp)
target_link_libraries(sequencer PRIVATE exchange-sequencer)
exchange_measured(sequencer)

add_executable(subgen test/subgen_main.cpp)
target_link_libraries(subgen PRIVATE exchange-sequencer matcher-test-harness)

# The two-hop driver: plays a gateway and a consumer at once, measuring driver write to first
# answering event across the live processes, closed loop (docs/PRACTICE.md, the campaign).
add_executable(driver test/driver_main.cpp)
target_link_libraries(driver PRIVATE exchange-sequencer matcher-test-harness)
exchange_measured(driver)

add_executable(sequencer-determinism test/determinism_test.cpp)
target_link_libraries(sequencer-determinism PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(sequencer-determinism PRIVATE
  SEQUENCER_BINARY="$<TARGET_FILE:sequencer>"
  SUBGEN_BINARY="$<TARGET_FILE:subgen>"
  MATCHER_BINARY="$<TARGET_FILE:matcher>"
  FLOWGEN_BINARY="$<TARGET_FILE:flowgen>")
add_dependencies(sequencer-determinism sequencer subgen matcher flowgen)

add_executable(sequencer-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(sequencer-allocation-probe PRIVATE exchange-sequencer matcher-test-harness)

add_executable(sequencer-allocation test/allocation_probe_test.cpp)
target_link_libraries(sequencer-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(sequencer-allocation PRIVATE
  SEQUENCER_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:sequencer-allocation-probe>")
add_dependencies(sequencer-allocation sequencer-allocation-probe)

add_executable(sequencer-benchmark src/benchmark_main.cpp)
target_link_libraries(sequencer-benchmark PRIVATE exchange-sequencer)
exchange_measured(sequencer-benchmark)

catch_discover_tests(sequencer-stream)
catch_discover_tests(sequencer-dedupe)
catch_discover_tests(sequencer-packets)
catch_discover_tests(sequencer-replication)
catch_discover_tests(sequencer-chaos)
catch_discover_tests(sequencer-sources)
catch_discover_tests(sequencer-determinism)
catch_discover_tests(sequencer-allocation)
