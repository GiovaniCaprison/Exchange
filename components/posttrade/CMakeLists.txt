# Post-trade: the day handed over. The ledger is header-only like the rest, the purest consumer
# in the venue, and its whole correctness claim is arithmetic.

add_library(exchange-posttrade INTERFACE)
target_include_directories(exchange-posttrade INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-posttrade INTERFACE exchange-protocol exchange-common)

# The suites borrow their neighbours: the matcher for the flow, the ecosystem's room for the
# venue-agrees-with-its-participants proof.
set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)
set(MARKETDATA_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../marketdata/test)
set(ECOSYSTEM_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../ecosystem/test)

add_executable(posttrade-ledger test/ledger_test.cpp)
target_link_libraries(posttrade-ledger
  PRIVATE exchange-posttrade exchange-matcher matcher-test-harness Catch2::Catch2WithMain)

add_executable(posttrade-agreement test/agreement_test.cpp)
target_include_directories(posttrade-agreement
  PRIVATE ${ECOSYSTEM_TEST_DIR} ${SEQUENCER_TEST_DIR} ${MARKETDATA_TEST_DIR}
          ${CMAKE_CURRENT_SOURCE_DIR}/../ecosystem/src)
target_link_libraries(posttrade-agreement
  PRIVATE exchange-posttrade exchange-ecosystem exchange-marketdata exchange-matcher
          matcher-test-harness Catch2::Catch2WithMain)

add_executable(posttrade src/posttrade_main.cpp)
target_link_libraries(posttrade PRIVATE exchange-posttrade)

add_executable(posttrade-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(posttrade-allocation-probe PRIVATE exchange-posttrade)

add_executable(posttrade-allocation test/allocation_probe_test.cpp)
target_link_libraries(posttrade-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(posttrade-allocation PRIVATE
  POSTTRADE_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:posttrade-allocation-probe>")
add_dependencies(posttrade-allocation posttrade-allocation-probe)

add_executable(posttrade-benchmark src/benchmark_main.cpp)
target_link_libraries(posttrade-benchmark PRIVATE exchange-posttrade)
exchange_measured(posttrade-benchmark)

catch_discover_tests(posttrade-ledger)
catch_discover_tests(posttrade-agreement)
catch_discover_tests(posttrade-allocation)
