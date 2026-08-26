// The safe policy's whole claim, proved from the outside: nothing is published or acknowledged
// before the standby's acknowledgment covers it, everything is once it is, the pipeline never
// stalls sequencing while it waits, and at every moment published is a subset of replicated. The
// standby's journal is held to byte identity with the primary's, which is the failover invariant
// as a diff.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "flow.hpp"
#include "journal.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-replication-" + name);
}

std::vector<char> stamped(const std::vector<CommandWriter::Framed>& flow) {
  std::vector<char> bytes;
  for (const CommandWriter::Framed& framed : flow) {
    bytes.insert(bytes.end(), framed.bytes.begin(), framed.bytes.end());
  }
  return bytes;
}

// The test-side standby: ranges in, a journal and grouped acknowledgments out, driven by hand so
// a suite can decide exactly when the link delivers.
struct WiredStandby {
  CapturingRing acks;
  common::journal::Writer journal;
  Standby<CapturingRing, common::journal::Writer> standby;

  explicit WiredStandby(const std::string& journalPath)
      : journal(journalPath), standby(journal, acks) {}
};

}  // namespace

TEST_CASE("nothing is published or acknowledged before the standby covers it") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(43, 600);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 2);
  const std::filesystem::path primaryPath = scratch("gate.exj");
  const std::filesystem::path standbyPath = scratch("gate.standby.exj");

  {
    Wired wired(primaryPath.string(), 2, Durability::SAFE);
    WiredStandby remote(standbyPath.string());

    // The primary sequences and ships everything with the link silent: the journal fills, the
    // pipeline holds, and the world hears nothing.
    for (std::vector<char>& record : records) {
      wired.sequencer.onSubmission(record.data(), record.size());
    }
    CHECK(wired.sequencer.sequence() == flow.size());
    CHECK(wired.sequencer.pending() == flow.size());
    CHECK(wired.sequencer.published() == 0);
    CHECK(wired.out.captured().empty());
    CHECK(readAcks(wired.acks[0].captured()).empty());
    CHECK(wired.link.ranges.size() == flow.size());

    // The link delivers half; only that half publishes, in order, published inside replicated.
    const std::size_t half = flow.size() / 2;
    for (std::size_t at = 0; at < half; at++) {
      remote.standby.onRange(wired.link.ranges[at].data(), wired.link.ranges[at].size());
    }
    const std::vector<std::uint64_t> covered = readReplicationAcks(remote.acks.captured());
    REQUIRE(!covered.empty());
    wired.sequencer.onReplicationAck(covered.back());
    CHECK(wired.sequencer.published() == half);
    CHECK(wired.sequencer.pending() == flow.size() - half);
    CHECK(remote.standby.held() == half);
    CHECK(wired.sequencer.published() <= remote.standby.held());

    // The rest lands and the stream is whole: bytes, order and acknowledgments all exact.
    for (std::size_t at = half; at < flow.size(); at++) {
      remote.standby.onRange(wired.link.ranges[at].data(), wired.link.ranges[at].size());
    }
    wired.sequencer.onReplicationAck(readReplicationAcks(remote.acks.captured()).back());
    CHECK(wired.sequencer.pending() == 0);
    CHECK(wired.out.captured() == stamped(flow));
    for (std::uint32_t gateway = 0; gateway < 2; gateway++) {
      const std::vector<Ack> acks = readAcks(wired.acks[gateway].captured());
      std::size_t seen = 0;
      for (std::size_t at = gateway; at < flow.size(); at += 2) {
        REQUIRE(seen < acks.size());
        CHECK(acks[seen].sequence == at + 1);
        seen++;
      }
    }
  }

  // The failover invariant as a diff: the standby's journal is the primary's, byte for byte.
  common::journal::Read primary = common::journal::read(primaryPath.string());
  common::journal::Read standby = common::journal::read(standbyPath.string());
  CHECK(primary.messages == standby.messages);
  CHECK(primary.messages == stamped(flow));
  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("one grouped acknowledgment publishes the whole flight in order") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(47, 200);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  const std::filesystem::path primaryPath = scratch("grouped.exj");
  const std::filesystem::path standbyPath = scratch("grouped.standby.exj");
  Wired wired(primaryPath.string(), 1, Durability::SAFE);
  WiredStandby remote(standbyPath.string());

  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  for (const std::vector<char>& range : wired.link.ranges) {
    std::vector<char> copy = range;
    remote.standby.onRange(copy.data(), copy.size());
  }
  // Only the last acknowledgment arrives; the group covers everything before it.
  wired.sequencer.onReplicationAck(readReplicationAcks(remote.acks.captured()).back());
  CHECK(wired.sequencer.pending() == 0);
  CHECK(wired.sequencer.published() == flow.size());
  CHECK(wired.out.captured() == stamped(flow));
  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("a retry of an in-flight command waits for durability") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(53, 20);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  const std::filesystem::path primaryPath = scratch("inflight.exj");
  const std::filesystem::path standbyPath = scratch("inflight.standby.exj");
  Wired wired(primaryPath.string(), 1, Durability::SAFE);
  WiredStandby remote(standbyPath.string());

  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  // The gateway retries submission 5 while it sits in the pipeline: no answer yet, because an
  // acknowledgment promises durability the standby has not confirmed.
  std::vector<char> retry = submissionRecord(0, 5, flow[4].bytes);
  wired.sequencer.onSubmission(retry.data(), retry.size());
  CHECK(readAcks(wired.acks[0].captured()).empty());
  CHECK(wired.sequencer.dropped() == 1);

  // Once covered, the same retry is answered from the window.
  for (const std::vector<char>& range : wired.link.ranges) {
    std::vector<char> copy = range;
    remote.standby.onRange(copy.data(), copy.size());
  }
  wired.sequencer.onReplicationAck(readReplicationAcks(remote.acks.captured()).back());
  std::vector<char> again = submissionRecord(0, 5, flow[4].bytes);
  wired.sequencer.onSubmission(again.data(), again.size());
  const std::vector<Ack> acks = readAcks(wired.acks[0].captured());
  CHECK(acks.size() == flow.size() + 1);
  CHECK(acks.back().gatewaySequence == 5);
  CHECK(acks.back().sequence == 5);
  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("the standby refuses a gap on the lossless link") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(59, 30);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  const std::filesystem::path primaryPath = scratch("linkgap.exj");
  const std::filesystem::path standbyPath = scratch("linkgap.standby.exj");
  Wired wired(primaryPath.string(), 1, Durability::SAFE);
  WiredStandby remote(standbyPath.string());
  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  // Range 0 lands, range 1 vanishes, range 2 arrives: refused, and the watermark stands still.
  remote.standby.onRange(wired.link.ranges[0].data(), wired.link.ranges[0].size());
  remote.standby.onRange(wired.link.ranges[2].data(), wired.link.ranges[2].size());
  CHECK(remote.standby.held() == 1);
  CHECK(remote.standby.violations() == 1);
  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("end of session travels the link and closes the standby") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(61, 10);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  const std::filesystem::path primaryPath = scratch("linkend.exj");
  const std::filesystem::path standbyPath = scratch("linkend.standby.exj");
  Wired wired(primaryPath.string(), 1, Durability::SAFE);
  WiredStandby remote(standbyPath.string());
  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
    remote.standby.onRange(wired.link.ranges.back().data(), wired.link.ranges.back().size());
    wired.sequencer.onReplicationAck(readReplicationAcks(remote.acks.captured()).back());
  }
  wired.sequencer.endSession();
  remote.standby.onRange(wired.link.ranges.back().data(), wired.link.ranges.back().size());
  CHECK(remote.standby.ended());
  CHECK(remote.standby.held() == flow.size());
  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}
