# The gateway: where clients meet the venue. Header-only like the rest of the hot path, so the
# reassembly, the session machine and the identity stamp inline into the process binary.

add_library(exchange-gateway INTERFACE)
target_include_directories(exchange-gateway INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# The suites borrow the sequencer's test machinery too: its capturing carriers and its wired
# sequencer are how the failover story is proved from the client's chair.
set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)
target_link_libraries(exchange-gateway INTERFACE exchange-protocol exchange-common
                                                 exchange-sequencer exchange-risk)

add_executable(gateway-session test/session_test.cpp)
target_include_directories(gateway-session PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(gateway-session
  PRIVATE exchange-gateway matcher-test-harness Catch2::Catch2WithMain)

add_executable(gateway-routing test/routing_test.cpp)
target_include_directories(gateway-routing PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(gateway-routing
  PRIVATE exchange-gateway matcher-test-harness Catch2::Catch2WithMain)

add_executable(gateway-resubmission test/resubmission_test.cpp)
target_include_directories(gateway-resubmission PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(gateway-resubmission
  PRIVATE exchange-gateway matcher-test-harness Catch2::Catch2WithMain)

add_executable(gateway-hostile test/hostile_test.cpp)
target_include_directories(gateway-hostile PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(gateway-hostile
  PRIVATE exchange-gateway matcher-test-harness Catch2::Catch2WithMain)

# The process itself, and the whole of it held to its word over a real socket.
add_executable(gateway src/gateway_main.cpp)
target_link_libraries(gateway PRIVATE exchange-gateway)
exchange_measured(gateway)

add_executable(gateway-socket test/socket_test.cpp)
target_include_directories(gateway-socket PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(gateway-socket
  PRIVATE exchange-gateway matcher-test-harness Catch2::Catch2WithMain)
target_compile_definitions(gateway-socket PRIVATE GATEWAY_BINARY="$<TARGET_FILE:gateway>")
add_dependencies(gateway-socket gateway)

add_executable(gateway-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(gateway-allocation-probe PRIVATE exchange-gateway matcher-test-harness)

add_executable(gateway-allocation test/allocation_probe_test.cpp)
target_link_libraries(gateway-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(gateway-allocation PRIVATE
  GATEWAY_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:gateway-allocation-probe>")
add_dependencies(gateway-allocation gateway-allocation-probe)

add_executable(gateway-benchmark src/benchmark_main.cpp)
target_include_directories(gateway-benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(gateway-benchmark PRIVATE exchange-gateway matcher-test-harness)
exchange_measured(gateway-benchmark)

# The coverage-guided arm of the hostile suite, built only under the fuzzer flavour with clang.
if(EXCHANGE_FUZZ)
  add_executable(gateway-fuzz test/fuzz_reassembler_main.cpp)
  target_link_libraries(gateway-fuzz PRIVATE exchange-gateway)
  target_compile_options(gateway-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(gateway-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
endif()

catch_discover_tests(gateway-session)
catch_discover_tests(gateway-routing)
catch_discover_tests(gateway-resubmission)
catch_discover_tests(gateway-hostile)
catch_discover_tests(gateway-socket)
catch_discover_tests(gateway-allocation)
