// Zero steady-state allocation, measured rather than trusted (P-6, P-14): the probe replays a
// generated session against an allocator that counts every request after initialisation and fails
// on one. The control arm is the same probe told to misbehave, which is how the mechanism itself
// is proved: a probe that cannot fail proves nothing.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>

#include "flow.hpp"
#include "journal.hpp"

using namespace exchange::matcher;
using namespace exchange::matcher::test;

namespace {

std::filesystem::path writeJournal() {
  const std::filesystem::path path = std::filesystem::temp_directory_path() / "exchange-probe.exj";
  journal::Writer writer(path.string());
  for (const CommandWriter::Framed& framed : generatedFlow(31, 50'000)) {
    writer.append(framed.bytes.data(), static_cast<std::uint32_t>(framed.bytes.size()));
  }
  return path;
}

}  // namespace

TEST_CASE("the matcher never asks the allocator once the session is running") {
  const std::filesystem::path path = writeJournal();
  const std::string binary = ALLOCATION_PROBE_BINARY;
  CHECK(std::system((binary + " --journal " + path.string()).c_str()) == 0);
  CHECK(std::system((binary + " --journal " + path.string() + " --misbehave").c_str()) != 0);
  std::filesystem::remove(path);
}
