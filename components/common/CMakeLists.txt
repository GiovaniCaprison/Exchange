# The carriers every process shares: the ring, the journal, the snapshot envelope and the byte
# plumbing under them. Header-only, so each process instantiates them beside its own hot path.

add_library(exchange-common INTERFACE)
target_include_directories(exchange-common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(exchange-common INTERFACE exchange-protocol)
