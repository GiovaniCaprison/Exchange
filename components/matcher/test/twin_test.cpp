// The availability claim, held at the consumer's chair: two broadcast rings carrying the same
// per-partition event stream are the stream's A and B, a seat over both hears every event
// exactly once and in order wherever it arrives first, one twin dying mid-day changes nothing
// downstream, and a seat arriving late covers itself from whichever twin retains the past. The
// events are a real partition's, because the claim is about the venue's stream, not a toy's.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "flow.hpp"
#include "harness.hpp"
#include "partition.hpp"
#include "stream.hpp"

using namespace exchange::matcher;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;
namespace sbe = exchange::protocol;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-twin-" + name);
}

// A real day's events, one framed message at a time.
std::vector<std::vector<char>> realEvents(const std::uint64_t seed, const std::uint64_t commands) {
  CapturingRing events;
  Partition<CapturingRing> partition{events};
  for (const CommandWriter::Framed& command : generatedFlow(seed, commands)) {
    std::vector<char> bytes = command.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  }
  std::vector<std::vector<char>> messages;
  std::vector<char> copy(events.captured());
  std::size_t at = 0;
  while (at < copy.size()) {
    sbe::MessageHeader wrap;
    wrap.wrap(copy.data(), at, 0, copy.size());
    const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
    messages.emplace_back(copy.begin() + static_cast<long>(at),
                          copy.begin() + static_cast<long>(at + length));
    at += length;
  }
  return messages;
}

void put(common::BroadcastRing& ring, const std::vector<char>& message) {
  const std::size_t at = ring.claim(message.size());
  std::memcpy(ring.buffer() + at, message.data(), message.size());
  ring.commit();
  ring.publish();
}

}  // namespace

TEST_CASE("a seat over both twins hears one stream, and a twin dying is a non-event") {
  const std::vector<std::vector<char>> stream = realEvents(311, 4000);
  REQUIRE(stream.size() > 1000);
  const std::filesystem::path pathA = scratch("a.ring");
  const std::filesystem::path pathB = scratch("b.ring");
  common::BroadcastRing twinA = common::BroadcastRing::create(pathA.string(), 1 << 24);
  common::BroadcastRing twinB = common::BroadcastRing::create(pathB.string(), 1 << 24);

  common::stream::SequencedSeat seat;
  seat.join(common::BroadcastReader::attach(pathA.string()));
  seat.join(common::BroadcastReader::attach(pathB.string()));

  // The primary leads and dies at the sixtieth percentile; the twin, a step behind the whole
  // way, says everything the primary ever said and keeps going. The seat neither misses one
  // event nor hears one twice.
  std::vector<std::vector<char>> heard;
  const auto listen = [&] {
    seat.poll([&](char* message, const std::size_t length) {
      heard.emplace_back(message, message + length);
    });
  };
  const std::size_t death = (stream.size() * 6) / 10;
  for (std::size_t at = 0; at < stream.size(); at++) {
    if (at < death) {
      put(twinA, stream[at]);
    }
    if (at > 0) {
      put(twinB, stream[at - 1]);
    }
    if (at % 7 == 0) {
      listen();
    }
  }
  put(twinB, stream.back());
  listen();

  REQUIRE(heard.size() == stream.size());
  CHECK(heard == stream);

  // A seat arriving after the whole day covers itself once from whichever twin retains it.
  common::stream::SequencedSeat late;
  late.join(common::BroadcastReader::attach(pathA.string()));
  late.join(common::BroadcastReader::attach(pathB.string()));
  std::size_t caught = 0;
  bool ordered = true;
  std::uint64_t last = 0;
  late.poll([&](char* message, const std::size_t length) {
    std::uint64_t sequence = 0;
    std::memcpy(&sequence, message + sbe::MessageHeader::encodedLength(), sizeof sequence);
    ordered = ordered && sequence == last + 1;
    last = sequence;
    caught++;
    (void)length;
  });
  CHECK(caught == stream.size());
  CHECK(ordered);

  std::filesystem::remove(pathA);
  std::filesystem::remove(pathB);
}
