// The core proof: unstamped submissions in, the harness's exact bytes out. The matcher harness
// stamps sequence and timestamp by hand; the sequencer, fed the same commands dealt across
// gateways with the stamps zeroed, must reproduce that stream byte for byte on its ring, in its
// journal and across its packets, and every acknowledgment must name the place its command
// received.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "flow.hpp"
#include "journal.hpp"
#include "ranges.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-sequencer-" + name);
}

std::vector<char> stamped(const std::vector<CommandWriter::Framed>& flow) {
  std::vector<char> bytes;
  for (const CommandWriter::Framed& framed : flow) {
    bytes.insert(bytes.end(), framed.bytes.begin(), framed.bytes.end());
  }
  return bytes;
}

}  // namespace

TEST_CASE("the sequencer reproduces the hand-stamped stream byte for byte") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(11, 6000);
  const std::uint32_t gateways = 3;
  std::vector<std::vector<char>> records = dealtSubmissions(flow, gateways);
  const std::filesystem::path journalPath = scratch("stream.exj");

  {
    Wired wired(journalPath.string(), gateways);
    for (std::vector<char>& record : records) {
      wired.sequencer.onSubmission(record.data(), record.size());
    }
    wired.sequencer.flush();

    const std::vector<char> expected = stamped(flow);
    CHECK(wired.sequencer.sequence() == flow.size());
    CHECK(wired.sequencer.duplicates() == 0);
    CHECK(wired.sequencer.dropped() == 0);
    CHECK(wired.sequencer.violations() == 0);

    // The ring carries the stream, one publish per command.
    CHECK(wired.out.captured() == expected);
    CHECK(wired.out.publishes() == flow.size());

    // The packets carry the same stream, each range opening at its first message's sequence.
    std::vector<char> fromPackets;
    for (std::vector<char>& packet : wired.packets.packets) {
      common::ranges::Reader reader(packet.data(), packet.size());
      CHECK(reader.epoch() == 1);
      std::uint64_t sequence = reader.firstSequence();
      reader.forEach([&](char* bytes, const std::size_t length) {
        std::uint64_t stampedSequence = 0;
        std::memcpy(&stampedSequence, bytes + exchange::protocol::MessageHeader::encodedLength(),
                    sizeof stampedSequence);
        CHECK(stampedSequence == sequence);
        sequence++;
        fromPackets.insert(fromPackets.end(), bytes, bytes + length);
      });
    }
    CHECK(fromPackets == expected);

    // Every acknowledgment names the place its command received and the timestamp it was given.
    for (std::uint32_t gateway = 0; gateway < gateways; gateway++) {
      const std::vector<Ack> acks = readAcks(wired.acks[gateway].captured());
      std::size_t seen = 0;
      for (std::size_t at = gateway; at < flow.size(); at += gateways) {
        REQUIRE(seen < acks.size());
        CHECK(acks[seen].gatewaySequence == seen + 1);
        CHECK(acks[seen].sequence == at + 1);
        CHECK(acks[seen].timestamp == 1'000'000'000'000ULL + at + 1);
        seen++;
      }
      CHECK(seen == acks.size());
    }
  }

  // The journal holds the same bytes the ring carried.
  common::journal::Read log = common::journal::read(journalPath.string());
  CHECK(log.count() == flow.size());
  CHECK(log.messages == stamped(flow));
  std::filesystem::remove(journalPath);
}

TEST_CASE("heartbeats name the next sequence and end of session closes the stream") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(7, 40);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);
  const std::filesystem::path journalPath = scratch("heartbeat.exj");
  Wired wired(journalPath.string(), 1);
  for (std::vector<char>& record : records) {
    wired.sequencer.onSubmission(record.data(), record.size());
  }
  wired.sequencer.heartbeat();
  wired.sequencer.endSession();

  REQUIRE(wired.packets.packets.size() >= 3);
  const std::size_t last = wired.packets.packets.size() - 1;
  common::ranges::Reader heartbeat(wired.packets.packets[last - 1].data(),
                                   wired.packets.packets[last - 1].size());
  CHECK(heartbeat.heartbeat());
  CHECK(heartbeat.firstSequence() == flow.size() + 1);
  common::ranges::Reader end(wired.packets.packets[last].data(),
                             wired.packets.packets[last].size());
  CHECK(end.endOfSession());
  CHECK(end.firstSequence() == flow.size() + 1);
  std::filesystem::remove(journalPath);
}
