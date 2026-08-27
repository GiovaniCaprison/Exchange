// The probe's own test: the honest run answers zero, and the control arm that allocates on
// purpose is caught, because a probe that cannot fail proves nothing.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>

TEST_CASE("oversight never asks the allocator once the day is running") {
  const std::string binary = OVERSIGHT_ALLOCATION_PROBE_BINARY;
  CHECK(std::system(binary.c_str()) == 0);
  CHECK(std::system((binary + " --misbehave").c_str()) != 0);
}
