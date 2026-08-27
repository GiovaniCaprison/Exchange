# The risk component: the gate the gateway consults before anything can cost a place in the
# global order. Header-only like the rest of the client path.

add_library(exchange-risk INTERFACE)
target_include_directories(exchange-risk INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-risk INTERFACE exchange-protocol exchange-sequencer)

set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)
set(GATEWAY_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../gateway/test)

add_executable(risk-checks test/checks_test.cpp)
target_link_libraries(risk-checks PRIVATE exchange-risk matcher-test-harness Catch2::Catch2WithMain)

add_executable(risk-exposure test/exposure_test.cpp)
target_link_libraries(risk-exposure
  PRIVATE exchange-risk exchange-matcher matcher-test-harness Catch2::Catch2WithMain)

# The gate proven where it stands: inside the gateway, refusing on the session.
add_executable(risk-gate test/gate_test.cpp)
target_include_directories(risk-gate PRIVATE ${SEQUENCER_TEST_DIR} ${GATEWAY_TEST_DIR})
target_link_libraries(risk-gate
  PRIVATE exchange-risk exchange-gateway matcher-test-harness Catch2::Catch2WithMain)

add_executable(risk-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(risk-allocation-probe PRIVATE exchange-risk matcher-test-harness)

add_executable(risk-allocation test/allocation_probe_test.cpp)
target_link_libraries(risk-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(risk-allocation PRIVATE
  RISK_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:risk-allocation-probe>")
add_dependencies(risk-allocation risk-allocation-probe)

add_executable(risk-benchmark src/benchmark_main.cpp)
target_link_libraries(risk-benchmark PRIVATE exchange-risk matcher-test-harness)
exchange_measured(risk-benchmark)

catch_discover_tests(risk-checks)
catch_discover_tests(risk-exposure)
catch_discover_tests(risk-gate)
catch_discover_tests(risk-allocation)
