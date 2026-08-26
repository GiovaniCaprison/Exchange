# The codecs, generated from the one schema every process shares.
#
# The generator is a Java tool, so it is fetched by exact version and checksum rather than expected
# to be installed. Both jars land in the build directory, which makes every build after the first
# one offline. The codecs build with every build, so a schema that stops generating fails the
# build rather than waiting for a consumer to notice.

set(SBE_VERSION 1.34.1)
set(SBE_SHA256 f510861f1307e8480a86748b09fdc1c8e5a6164ee306f00b0becdfe4885e740d)
set(AGRONA_VERSION 2.2.1)
set(AGRONA_SHA256 49d7dd589b5cdd3aa3ae57212c244c8d2f0c42e2b4c7a1de86f2571825662ee7)

set(MAVEN_CENTRAL https://repo1.maven.org/maven2)
set(JARS ${CMAKE_CURRENT_BINARY_DIR}/tools)
set(SBE_JAR ${JARS}/sbe-tool-${SBE_VERSION}.jar)
set(AGRONA_JAR ${JARS}/agrona-${AGRONA_VERSION}.jar)

find_package(Java REQUIRED COMPONENTS Runtime)

file(DOWNLOAD
  ${MAVEN_CENTRAL}/uk/co/real-logic/sbe-tool/${SBE_VERSION}/sbe-tool-${SBE_VERSION}.jar
  ${SBE_JAR}
  EXPECTED_HASH SHA256=${SBE_SHA256})

file(DOWNLOAD
  ${MAVEN_CENTRAL}/org/agrona/agrona/${AGRONA_VERSION}/agrona-${AGRONA_VERSION}.jar
  ${AGRONA_JAR}
  EXPECTED_HASH SHA256=${AGRONA_SHA256})

set(EXCHANGE_SCHEMA ${CMAKE_CURRENT_SOURCE_DIR}/exchange.xml)
set(GENERATED ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(SENTINEL ${GENERATED}/exchange_protocol/NewOrder.h)

add_custom_command(
  OUTPUT ${SENTINEL}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATED}
  COMMAND ${Java_JAVA_EXECUTABLE}
          -Dsbe.output.dir=${GENERATED}
          -Dsbe.target.language=Cpp
          -cp ${SBE_JAR}:${AGRONA_JAR}
          uk.co.real_logic.sbe.SbeTool
          ${EXCHANGE_SCHEMA}
  DEPENDS ${EXCHANGE_SCHEMA}
  COMMENT "Generating C++ codecs from the schema"
  VERBATIM)

add_custom_target(exchange-protocol-codecs ALL DEPENDS ${SENTINEL})

add_library(exchange-protocol INTERFACE)
add_dependencies(exchange-protocol exchange-protocol-codecs)
target_include_directories(exchange-protocol INTERFACE ${GENERATED})

# The codecs' bounds checks leave the release build: input below a process's boundary arrives
# well framed by contract (ARCHITECTURE.md, what a process may assume), so re-checking it per
# field is a branch the hot path does not owe. The debug and sanitizer flavours keep the checks,
# which makes trust a build type rather than a hope.
target_compile_definitions(exchange-protocol INTERFACE $<$<CONFIG:Release>:SBE_NO_BOUNDS_CHECK>)

# The round trip: every message encoded through its generated encoder and read back through its
# decoder, so the schema, the contexts and the field layout in docs/PROTOCOL.md are held to each
# other by a test rather than by attention.
add_executable(schema-round-trip round_trip_test.cpp)
target_link_libraries(schema-round-trip PRIVATE exchange-protocol Catch2::Catch2WithMain)

include(Catch)
catch_discover_tests(schema-round-trip)
