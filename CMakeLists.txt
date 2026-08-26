cmake_minimum_required(VERSION 3.25)
project(exchange CXX)

# One target per component as they arrive, plus the schema they share. The schema and the corpus
# sit at the top so no component owns the contract.

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  # A latency component built without optimisation measures the debug build. Release is the only
  # default that is not a mistake here.
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# Points git at the committed hooks on the first configure, so formatting on commit installs
# itself for anyone who builds. Git never activates hooks from a clone on its own, which is a
# security decision and not an omission, so some build has to do this once.
execute_process(
  COMMAND git config core.hooksPath hooks
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

enable_testing()
