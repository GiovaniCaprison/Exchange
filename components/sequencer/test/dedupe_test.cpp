// Exactly-once is retry plus deduplication, so the dedupe is proved from the retry's side: a
// resubmission is re-acknowledged with the place its command already holds and sequences
// nothing; a gap is refused; a retry older than the window is dropped rather than guessed at;
// and gateways number independently, so the same gatewaySequence from two gateways is two
// commands.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "flow.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-dedupe-" + name);
}

}  // namespace

TEST_CASE("a resubmission is re-acknowledged and never re-sequenced") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(3, 20);
  const std::filesystem::path journalPath = scratch("retry.exj");
  Wired wired(journalPath.string(), 1);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  const std::uint64_t sequenced = wired.sequencer.sequence();
  const std::size_t published = wired.out.captured().size();

  // The gateway saw no acks for its 5th and 9th submissions and retries them.
  std::vector<char> retry5 = submissionRecord(0, 5, flow[4].bytes);
  std::vector<char> retry9 = submissionRecord(0, 9, flow[8].bytes);
  wired.sequencer.onSubmission(retry5.data(), retry5.size());
  wired.sequencer.onSubmission(retry9.data(), retry9.size());

  CHECK(wired.sequencer.sequence() == sequenced);
  CHECK(wired.sequencer.duplicates() == 2);
  CHECK(wired.out.captured().size() == published);

  const std::vector<Ack> acks = readAcks(wired.acks[0].captured());
  REQUIRE(acks.size() == flow.size() + 2);
  CHECK(acks[flow.size()].gatewaySequence == 5);
  CHECK(acks[flow.size()].sequence == acks[4].sequence);
  CHECK(acks[flow.size()].timestamp == acks[4].timestamp);
  CHECK(acks[flow.size() + 1].gatewaySequence == 9);
  CHECK(acks[flow.size() + 1].sequence == acks[8].sequence);
  std::filesystem::remove(journalPath);
}

TEST_CASE("a gap on the submission carrier is refused") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(5, 10);
  const std::filesystem::path journalPath = scratch("gap.exj");
  Wired wired(journalPath.string(), 1);
  std::vector<char> first = submissionRecord(0, 1, flow[0].bytes);
  wired.sequencer.onSubmission(first.data(), first.size());
  // gatewaySequence 3 arrives with 2 never seen: a violated precondition, counted and refused.
  std::vector<char> skipped = submissionRecord(0, 3, flow[1].bytes);
  wired.sequencer.onSubmission(skipped.data(), skipped.size());
  CHECK(wired.sequencer.sequence() == 1);
  CHECK(wired.sequencer.dropped() == 1);
  CHECK(readAcks(wired.acks[0].captured()).size() == 1);
  std::filesystem::remove(journalPath);
}

TEST_CASE("a retry older than the window is dropped rather than answered") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(9, 1500);
  const std::filesystem::path journalPath = scratch("window.exj");
  Wired wired(journalPath.string(), 1);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  REQUIRE(flow.size() > wired.sequencer.WINDOW);
  const std::size_t acked = readAcks(wired.acks[0].captured()).size();

  // Submission 1's slot was overwritten by submission 1025; the retry cannot be answered.
  std::vector<char> ancient = submissionRecord(0, 1, flow[0].bytes);
  wired.sequencer.onSubmission(ancient.data(), ancient.size());
  CHECK(wired.sequencer.dropped() == 1);
  CHECK(readAcks(wired.acks[0].captured()).size() == acked);

  // A retry still inside the window is answered.
  std::vector<char> recent = submissionRecord(0, flow.size(), flow[flow.size() - 1].bytes);
  wired.sequencer.onSubmission(recent.data(), recent.size());
  CHECK(wired.sequencer.duplicates() == 1);
  CHECK(readAcks(wired.acks[0].captured()).size() == acked + 1);
  std::filesystem::remove(journalPath);
}

TEST_CASE("gateways number independently") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(13, 8);
  const std::filesystem::path journalPath = scratch("independent.exj");
  Wired wired(journalPath.string(), 2);
  std::vector<char> a = submissionRecord(0, 1, flow[0].bytes);
  std::vector<char> b = submissionRecord(1, 1, flow[1].bytes);
  wired.sequencer.onSubmission(a.data(), a.size());
  wired.sequencer.onSubmission(b.data(), b.size());
  CHECK(wired.sequencer.sequence() == 2);
  CHECK(wired.sequencer.duplicates() == 0);
  CHECK(readAcks(wired.acks[0].captured()).size() == 1);
  CHECK(readAcks(wired.acks[1].captured()).size() == 1);
  std::filesystem::remove(journalPath);
}

TEST_CASE("a record that is not a submission is a violation") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(17, 4);
  const std::filesystem::path journalPath = scratch("violation.exj");
  Wired wired(journalPath.string(), 1);
  // A bare command without its envelope, and an envelope naming a gateway that does not exist.
  std::vector<char> bare = flow[0].bytes;
  wired.sequencer.onSubmission(bare.data(), bare.size());
  std::vector<char> stranger = submissionRecord(7, 1, flow[1].bytes);
  wired.sequencer.onSubmission(stranger.data(), stranger.size());
  CHECK(wired.sequencer.violations() == 2);
  CHECK(wired.sequencer.sequence() == 0);
  std::filesystem::remove(journalPath);
}
