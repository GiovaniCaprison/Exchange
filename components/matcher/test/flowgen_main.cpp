// Writes a generated session as a journal, so the matcher, the probe and the benchmark all replay
// the same bytes:
//
//   flowgen --journal J [--commands N] [--seed S]

#include <cstdint>
#include <cstdio>
#include <string>

#include "flow.hpp"
#include "journal.hpp"

int main(const int count, char** values) {
  using namespace exchange::matcher;
  using namespace exchange::matcher::test;
  std::string journalPath;
  std::uint64_t commands = 100'000;
  std::uint64_t seed = 1;
  for (int at = 1; at + 1 < count; at++) {
    const std::string name = values[at];
    if (name == "--journal") {
      journalPath = values[at + 1];
    } else if (name == "--commands") {
      commands = std::stoull(values[at + 1]);
    } else if (name == "--seed") {
      seed = std::stoull(values[at + 1]);
    }
  }
  if (journalPath.empty()) {
    std::fprintf(stderr, "usage: flowgen --journal J [--commands N] [--seed S]\n");
    return 2;
  }
  journal::Writer writer(journalPath);
  for (const CommandWriter::Framed& framed : generatedFlow(seed, commands)) {
    writer.append(framed.bytes.data(), static_cast<std::uint32_t>(framed.bytes.size()));
  }
  return 0;
}
