# Oversight: the venue answerable. Drop copy and surveillance, header-only like the rest, both
# consumers of the same event stream as everyone.

add_library(exchange-oversight INTERFACE)
target_include_directories(exchange-oversight INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-oversight INTERFACE exchange-protocol exchange-common
                                                   exchange-gateway)

# The suites borrow their neighbours: the gateway's session client for the watcher's chair, the
# matcher for the scenarios, and the ecosystem's room for the innocent day.
set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)
set(GATEWAY_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../gateway/test)
set(MARKETDATA_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../marketdata/test)
set(ECOSYSTEM_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../ecosystem/test)

add_executable(oversight-surveillance test/surveillance_test.cpp)
target_link_libraries(oversight-surveillance
  PRIVATE exchange-oversight exchange-matcher matcher-test-harness Catch2::Catch2WithMain)

add_executable(oversight-dropcopy test/dropcopy_test.cpp)
target_include_directories(oversight-dropcopy PRIVATE ${GATEWAY_TEST_DIR} ${SEQUENCER_TEST_DIR})
target_link_libraries(oversight-dropcopy
  PRIVATE exchange-oversight matcher-test-harness Catch2::Catch2WithMain)

# The negative control: the ecosystem's seeded day, watched end to end, raises nothing.
add_executable(oversight-innocent test/innocent_test.cpp)
target_include_directories(oversight-innocent
  PRIVATE ${ECOSYSTEM_TEST_DIR} ${SEQUENCER_TEST_DIR} ${MARKETDATA_TEST_DIR}
          ${CMAKE_CURRENT_SOURCE_DIR}/../ecosystem/src)
target_link_libraries(oversight-innocent
  PRIVATE exchange-oversight exchange-ecosystem exchange-marketdata exchange-matcher
          matcher-test-harness Catch2::Catch2WithMain)

add_executable(surveillance src/surveillance_main.cpp)
target_link_libraries(surveillance PRIVATE exchange-oversight exchange-sequencer)
exchange_measured(surveillance)

add_executable(dropcopy src/dropcopy_main.cpp)
target_link_libraries(dropcopy PRIVATE exchange-oversight exchange-sequencer)

add_executable(oversight-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(oversight-allocation-probe PRIVATE exchange-oversight exchange-sequencer)

add_executable(oversight-allocation test/allocation_probe_test.cpp)
target_link_libraries(oversight-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(oversight-allocation PRIVATE
  OVERSIGHT_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:oversight-allocation-probe>")
add_dependencies(oversight-allocation oversight-allocation-probe)

add_executable(oversight-benchmark src/benchmark_main.cpp)
target_link_libraries(oversight-benchmark PRIVATE exchange-oversight)
exchange_measured(oversight-benchmark)

catch_discover_tests(oversight-surveillance)
catch_discover_tests(oversight-dropcopy)
catch_discover_tests(oversight-innocent)
catch_discover_tests(oversight-allocation)
