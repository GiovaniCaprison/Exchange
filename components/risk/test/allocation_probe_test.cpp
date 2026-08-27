// The probe's own test: the honest run answers zero, and the control arm that allocates on
// purpose is caught, because a probe that cannot fail proves nothing.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <string>

TEST_CASE("the gate never asks the allocator once the session is running") {
  const std::string binary = RISK_ALLOCATION_PROBE_BINARY;
  CHECK(std::system(binary.c_str()) == 0);
  CHECK(std::system((binary + " --misbehave").c_str()) != 0);
}
