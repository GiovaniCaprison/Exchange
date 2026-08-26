// The consumer library's contract: three sources, one stream. The same run consumed through the
// ring, the journal and the packet feed delivers identical bytes, which is what lets every
// downstream process choose its carrier by situation (live, recovering, remote) rather than by
// behaviour.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "flow.hpp"
#include "rewinder.hpp"
#include "spsc_ring.hpp"
#include "stream.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-sources-" + name);
}

}  // namespace

TEST_CASE("the ring, the journal and the packet feed deliver identical bytes") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(37, 4000);
  const std::filesystem::path journalPath = scratch("run.exj");
  const std::filesystem::path ringPath = scratch("run.ring");

  // The run, publishing to a real mapped ring this time.
  std::vector<char> expected;
  std::vector<std::vector<char>> packets;
  {
    common::SpscRing out = common::SpscRing::create(ringPath.string(), 1 << 21);
    std::vector<CapturingRing> acks(2);
    CapturingPacketSink sink;
    common::journal::Writer journal(journalPath.string());
    ScriptedClock clock;
    Sequencer<common::SpscRing, CapturingRing, CapturingPacketSink, common::journal::Writer,
              ScriptedClock>
        sequencer(out, acks, sink, journal, clock);
    std::vector<std::vector<char>> records = dealtSubmissions(flow, 2);
    for (std::vector<char>& record : records) {
      sequencer.onSubmission(record.data(), record.size());
    }
    sequencer.endSession();
    packets = sink.packets;
    for (const CommandWriter::Framed& framed : flow) {
      expected.insert(expected.end(), framed.bytes.begin(), framed.bytes.end());
    }
  }

  const auto collect = [](auto&& deliver) {
    std::vector<char> bytes;
    deliver([&](char* message, const std::size_t length) {
      bytes.insert(bytes.end(), message, message + length);
    });
    return bytes;
  };

  common::SpscRing ring = common::SpscRing::attach(ringPath.string());
  common::stream::RingSource ringSource(ring);
  const std::vector<char> fromRing = collect([&](auto&& handler) { ringSource.poll(handler); });

  common::stream::JournalSource journalSource(journalPath.string());
  const std::vector<char> fromJournal =
      collect([&](auto&& handler) { journalSource.poll(handler); });

  RewindChannel channel;
  Rewinder rewinder(journalPath.string());
  common::stream::PacketSource<RewindChannel> packetSource(1, 1, channel);
  const std::vector<char> fromPackets = collect([&](auto&& handler) {
    for (std::vector<char>& packet : packets) {
      packetSource.onPacket(packet.data(), packet.size(), handler);
    }
  });

  CHECK(fromRing == expected);
  CHECK(fromJournal == expected);
  CHECK(fromPackets == expected);
  CHECK(packetSource.ended());

  std::filesystem::remove(journalPath);
  std::filesystem::remove(ringPath);
}
