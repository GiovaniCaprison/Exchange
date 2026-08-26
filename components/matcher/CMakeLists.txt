# The matcher's engine core, header-only on purpose: the whole hot path lives in templates the
# process binary instantiates once, so claim, commit and every book operation inline with no
# dispatched call anywhere in the match loop (P-13).

add_library(exchange-matcher INTERFACE)
target_include_directories(exchange-matcher INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(exchange-matcher INTERFACE exchange-protocol)

add_library(matcher-test-harness STATIC test/harness.cpp)
target_include_directories(matcher-test-harness PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/test)
target_link_libraries(matcher-test-harness PUBLIC exchange-matcher)

add_executable(matcher-corpus test/corpus_test.cpp)
target_link_libraries(matcher-corpus PRIVATE matcher-test-harness Catch2::Catch2WithMain)

add_executable(matcher-invariants test/invariants_test.cpp)
target_link_libraries(matcher-invariants PRIVATE matcher-test-harness Catch2::Catch2WithMain)

add_executable(matcher-attribution test/attribution_test.cpp)
target_link_libraries(matcher-attribution PRIVATE matcher-test-harness Catch2::Catch2WithMain)

catch_discover_tests(matcher-corpus)
catch_discover_tests(matcher-invariants)
catch_discover_tests(matcher-attribution)
