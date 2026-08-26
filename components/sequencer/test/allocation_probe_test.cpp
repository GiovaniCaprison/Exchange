// The probe's own test: the honest run answers zero, and the control arm that allocates on
// purpose is caught, because a probe that cannot fail proves nothing.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-seq-allocation-" + name);
}

}  // namespace

TEST_CASE("the sequencer never asks the allocator once the session is running") {
  const std::filesystem::path journal = scratch("probe.exj");
  const std::string binary = SEQUENCER_ALLOCATION_PROBE_BINARY;
  CHECK(std::system((binary + " --journal " + journal.string()).c_str()) == 0);
  CHECK(std::system((binary + " --journal " + journal.string() + " --misbehave").c_str()) != 0);
  std::filesystem::remove(journal);
}
