// Writes the submission-plane input every binary proof shares: the harness's generated flow with
// its stamps zeroed, dealt round robin across gateways, as a journal-format file whose records
// are GatewaySubmission plus command. The same seed through flowgen writes the stamped stream
// the sequencer must reproduce, which is what makes the two files diffable ends of one claim.
//
//   subgen --submissions FILE [--gateways G] [--commands N] [--seed S]

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "flow.hpp"
#include "journal.hpp"
#include "submission.hpp"

int main(const int count, char** values) {
  namespace common = exchange::common;
  namespace test = exchange::sequencer::test;

  std::string submissionsPath;
  std::uint32_t gateways = 3;
  std::uint64_t commands = 50'000;
  std::uint64_t seed = 20260826;
  for (int at = 1; at + 1 < count; at++) {
    const std::string name = values[at];
    if (name == "--submissions") {
      submissionsPath = values[at + 1];
    } else if (name == "--gateways") {
      gateways = static_cast<std::uint32_t>(std::stoul(values[at + 1]));
    } else if (name == "--commands") {
      commands = std::stoull(values[at + 1]);
    } else if (name == "--seed") {
      seed = std::stoull(values[at + 1]);
    }
  }
  if (submissionsPath.empty()) {
    std::fprintf(stderr,
                 "usage: subgen --submissions FILE [--gateways G] [--commands N] [--seed S]\n");
    return 2;
  }

  const std::vector<exchange::matcher::test::CommandWriter::Framed> flow =
      exchange::matcher::test::generatedFlow(seed, commands);
  common::journal::Writer writer(submissionsPath);
  for (const std::vector<char>& record : test::dealtSubmissions(flow, gateways)) {
    writer.append(record.data(), static_cast<std::uint32_t>(record.size()));
  }
  return 0;
}
