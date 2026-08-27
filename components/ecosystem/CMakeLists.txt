# The ecosystem: the participant's seat. The feed handler and the order entry client are the
# venue's protocols spoken from the other side, header-only like everything on a hot path.

add_library(exchange-ecosystem INTERFACE)
target_include_directories(exchange-ecosystem INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-ecosystem INTERFACE exchange-protocol exchange-common
                                                   exchange-gateway)

# The suites hold the clients to the real venue: the marketdata machinery for the feed side, the
# sequencer's wired venue and the gateway machine for the session side.
set(SEQUENCER_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../sequencer/test)
set(MARKETDATA_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../marketdata/test)

add_executable(ecosystem-feedhandler test/feedhandler_test.cpp)
target_include_directories(ecosystem-feedhandler
  PRIVATE ${SEQUENCER_TEST_DIR} ${MARKETDATA_TEST_DIR})
target_link_libraries(ecosystem-feedhandler
  PRIVATE exchange-ecosystem exchange-marketdata matcher-test-harness Catch2::Catch2WithMain)

add_executable(ecosystem-orderentry test/orderentry_test.cpp)
target_include_directories(ecosystem-orderentry PRIVATE ${SEQUENCER_TEST_DIR})
target_link_libraries(ecosystem-orderentry
  PRIVATE exchange-ecosystem exchange-matcher matcher-test-harness Catch2::Catch2WithMain)

add_executable(ecosystem-bots test/bots_test.cpp)
target_include_directories(ecosystem-bots PRIVATE ${SEQUENCER_TEST_DIR} ${MARKETDATA_TEST_DIR})
target_link_libraries(ecosystem-bots
  PRIVATE exchange-ecosystem exchange-marketdata exchange-matcher matcher-test-harness
          Catch2::Catch2WithMain)

add_executable(ecosystem-allocation-probe test/allocation_probe_main.cpp)
target_link_libraries(ecosystem-allocation-probe PRIVATE exchange-ecosystem exchange-sequencer)

add_executable(ecosystem-allocation test/allocation_probe_test.cpp)
target_link_libraries(ecosystem-allocation PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(ecosystem-allocation PRIVATE
  ECOSYSTEM_ALLOCATION_PROBE_BINARY="$<TARGET_FILE:ecosystem-allocation-probe>")
add_dependencies(ecosystem-allocation ecosystem-allocation-probe)

# The participant process, and the whole venue held to its word over real wires.
add_executable(bot src/bot_main.cpp)
target_link_libraries(bot PRIVATE exchange-ecosystem exchange-sequencer)
exchange_measured(bot)

add_executable(ecosystem-day test/day_socket_test.cpp)
target_link_libraries(ecosystem-day PRIVATE Catch2::Catch2WithMain)
target_compile_definitions(ecosystem-day PRIVATE
  BOT_BINARY="$<TARGET_FILE:bot>"
  GATEWAY_BINARY="$<TARGET_FILE:gateway>"
  SEQUENCER_BINARY="$<TARGET_FILE:sequencer>"
  MATCHER_BINARY="$<TARGET_FILE:matcher>"
  MARKETDATA_BINARY="$<TARGET_FILE:marketdata>"
  OPERATIONS_BINARY="$<TARGET_FILE:operations>")
add_dependencies(ecosystem-day bot gateway sequencer matcher marketdata operations)

catch_discover_tests(ecosystem-feedhandler)
catch_discover_tests(ecosystem-orderentry)
catch_discover_tests(ecosystem-bots)
catch_discover_tests(ecosystem-day)
catch_discover_tests(ecosystem-allocation)
