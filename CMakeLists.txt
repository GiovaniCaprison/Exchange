cmake_minimum_required(VERSION 3.25)
project(exchange CXX)

# One target per component as they arrive, plus the schema they share. The schema and the corpus
# sit at the top so no component owns the contract.

# The compile database is how an editor's language server learns the include roots, the
# generated headers and the language standard, so configuring once makes every file readable in
# place. clangd finds it through .clangd beside this file.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  # A latency component built without optimisation measures the debug build. Release is the only
  # default that is not a mistake here.
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# The sanitizer flavour: address and undefined behaviour checking over the whole battery, run in
# ci beside the release build. Undefined behaviour is the bug class the optimiser is licensed to
# assume away, so it is hunted by a build whose only job is to see it. Recovery is off so a
# finding is an exit code, and the flags go on before the test framework is fetched because a
# sanitized binary must be sanitized all the way down.
option(EXCHANGE_SANITIZE "address and undefined behaviour sanitizers on every target" OFF)
option(EXCHANGE_FUZZ "build the coverage-guided fuzz targets (clang only)" OFF)
if(EXCHANGE_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all
                      -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# The campaign's flavours (docs/PRACTICE.md): link time optimisation and profile guidance on
# the measured binaries, with the whole build recorded into every manifest they write, so a
# number's toolchain is never a guess. Flags follow gcc's spelling with the one gcc-only flag
# guarded, since the box builds with either compiler.
option(EXCHANGE_LTO "link time optimisation for the measured binaries" OFF)
set(EXCHANGE_PGO "" CACHE STRING "profile guided optimisation: empty, generate, or use")
function(exchange_measured target)
  if(EXCHANGE_LTO)
    set_property(TARGET ${target} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  endif()
  if(EXCHANGE_PGO STREQUAL "generate")
    target_compile_options(${target} PRIVATE -fprofile-generate)
    target_link_options(${target} PRIVATE -fprofile-generate)
  elseif(EXCHANGE_PGO STREQUAL "use")
    target_compile_options(${target} PRIVATE -fprofile-use)
    target_link_options(${target} PRIVATE -fprofile-use)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      target_compile_options(${target} PRIVATE -fprofile-correction)
    endif()
  endif()
  target_compile_definitions(${target} PRIVATE
    EXCHANGE_BUILD_FLAGS="${CMAKE_BUILD_TYPE} LTO=${EXCHANGE_LTO} PGO=${EXCHANGE_PGO} CXXFLAGS=${CMAKE_CXX_FLAGS}")
endfunction()

# Points git at the committed hooks on the first configure, so formatting on commit installs
# itself for anyone who builds. Git never activates hooks from a clone on its own, which is a
# security decision and not an omission, so some build has to do this once.
execute_process(
  COMMAND git config core.hooksPath hooks
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

enable_testing()

# The test framework, fetched by exact tag so every machine tests with the same one. A test's name
# is prose, so it can say what it proves.
include(FetchContent)
FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.7.1
  GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(Catch2)
list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)

add_subdirectory(schema)
add_subdirectory(components/common)
add_subdirectory(components/matcher)
add_subdirectory(components/sequencer)
add_subdirectory(components/gateway)
