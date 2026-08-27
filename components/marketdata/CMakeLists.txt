# The market data component: the public feed derived from the matcher's events, header-only like
# the rest of the hot path.

add_library(exchange-marketdata INTERFACE)
target_include_directories(exchange-marketdata INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-marketdata INTERFACE exchange-protocol exchange-common
                                                    exchange-sequencer exchange-matcher)

# The suites borrow the matcher's harness for flows and the sequencer's for the rewind channel,
# because the feed's proofs are the venue's own machinery turned on itself.
set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)

add_executable(marketdata-rebuild test/rebuild_test.cpp)
target_include_directories(marketdata-rebuild PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-rebuild
  PRIVATE exchange-marketdata matcher-test-harness Catch2::Catch2WithMain)

add_executable(marketdata-glimpse test/glimpse_test.cpp)
target_include_directories(marketdata-glimpse PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-glimpse
  PRIVATE exchange-marketdata matcher-test-harness Catch2::Catch2WithMain)

add_executable(marketdata-ab test/ab_test.cpp)
target_include_directories(marketdata-ab PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-ab
  PRIVATE exchange-marketdata matcher-test-harness Catch2::Catch2WithMain)

# The process itself, and the whole of it held to its word over real sockets.
add_executable(marketdata src/marketdata_main.cpp)
target_link_libraries(marketdata PRIVATE exchange-marketdata)
exchange_measured(marketdata)

add_executable(marketdata-socket test/socket_test.cpp)
target_include_directories(marketdata-socket PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-socket
  PRIVATE exchange-marketdata matcher-test-harness Catch2::Catch2WithMain)
target_compile_definitions(marketdata-socket PRIVATE
  MARKETDATA_BINARY="$<TARGET_FILE:marketdata>")
add_dependencies(marketdata-socket marketdata)

add_executable(marketdata-allocation-probe test/allocation_probe_main.cpp)
target_include_directories(marketdata-allocation-probe PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-allocation-probe
  PRIVATE exchange-marketdata matcher-test-harness)

add_executable(marketdata-allocation test/allocation_probe_test.cpp)
target_link_libraries(marketdata-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(marketdata-allocation PRIVATE
  MARKETDATA_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:marketdata-allocation-probe>")
add_dependencies(marketdata-allocation marketdata-allocation-probe)

add_executable(marketdata-benchmark src/benchmark_main.cpp)
target_include_directories(marketdata-benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/test
                                                        ${SEQUENCER_TEST_DIR})
target_link_libraries(marketdata-benchmark PRIVATE exchange-marketdata matcher-test-harness)
exchange_measured(marketdata-benchmark)

catch_discover_tests(marketdata-rebuild)
catch_discover_tests(marketdata-glimpse)
catch_discover_tests(marketdata-ab)
catch_discover_tests(marketdata-socket)
catch_discover_tests(marketdata-allocation)
