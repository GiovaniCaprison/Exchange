# Market operations: the market's clock, a consumer with authority, header-only like the rest.

add_library(exchange-operations INTERFACE)
target_include_directories(exchange-operations INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-operations INTERFACE exchange-protocol exchange-common
                                                    exchange-sequencer)

set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)

add_executable(operations-schedule test/schedule_test.cpp)
target_include_directories(operations-schedule PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(operations-schedule
  PRIVATE exchange-operations matcher-test-harness Catch2::Catch2WithMain)

add_executable(operations-halt test/halt_test.cpp)
target_include_directories(operations-halt PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(operations-halt
  PRIVATE exchange-operations matcher-test-harness Catch2::Catch2WithMain)

# The flagship: a whole scripted day, twice, to identical journals, halts included.
add_executable(operations-day test/day_test.cpp)
target_include_directories(operations-day PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(operations-day
  PRIVATE exchange-operations exchange-matcher matcher-test-harness Catch2::Catch2WithMain)

add_executable(operations src/operations_main.cpp)
target_link_libraries(operations PRIVATE exchange-operations)
exchange_measured(operations)

add_executable(operations-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(operations-allocation-probe PRIVATE exchange-operations)

add_executable(operations-allocation test/allocation_probe_test.cpp)
target_link_libraries(operations-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(operations-allocation PRIVATE
  OPERATIONS_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:operations-allocation-probe>")
add_dependencies(operations-allocation operations-allocation-probe)

add_executable(operations-benchmark src/benchmark_main.cpp)
target_link_libraries(operations-benchmark PRIVATE exchange-operations)
exchange_measured(operations-benchmark)

catch_discover_tests(operations-schedule)
catch_discover_tests(operations-halt)
catch_discover_tests(operations-day)
catch_discover_tests(operations-allocation)
