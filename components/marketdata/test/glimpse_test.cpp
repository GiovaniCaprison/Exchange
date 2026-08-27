// Snapshot-then-join, proved as the equality it promises: a consumer who took the snapshot
// mid-session and then the live feed from the sequence it named ends with the same book as one
// who consumed the whole feed from its first packet, and the same book again as the venue's own.

#include "glimpse.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "feedtest.hpp"
#include "ranges.hpp"
#include "rewinder.hpp"
#include "stream.hpp"
#include "submission.hpp"

using namespace exchange::marketdata;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;
using exchange::sequencer::test::RewindChannel;

TEST_CASE("a late joiner's snapshot plus the live suffix equals the whole feed") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(73, 5000);
  const std::size_t cut = 2500;
  Venue venue;
  venue.play(flow, 0, cut);

  // The joiner connects now: snapshot taken, join sequence named.
  const std::uint64_t joinAt = venue.publisher.next();
  std::vector<std::vector<char>> snapshotPackets;
  snapshot(venue.builder, joinAt, [&](char* packet, const std::size_t length) {
    snapshotPackets.emplace_back(packet, packet + length);
  });

  // The session continues without them.
  venue.play(flow, cut, flow.size());

  // The joiner: snapshot messages first, then the live feed from joinAt, overlap skipped by the
  // consumer library's own sequencing.
  NaiveBook joiner;
  std::uint64_t joinNamed = 0;
  for (std::vector<char>& packet : snapshotPackets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach([&](char* message, const std::size_t size) {
      exchange::protocol::MessageHeader wrap;
      wrap.wrap(message, 0, 0, size);
      if (wrap.templateId() == exchange::protocol::SnapshotComplete::sbeTemplateId()) {
        exchange::protocol::SnapshotComplete complete;
        complete.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                               size);
        joinNamed = complete.nextSequence();
        return;
      }
      joiner.onPublic(message, size);
    });
  }
  REQUIRE(joinNamed == joinAt);

  RewindChannel channel;
  common::stream::PacketSource<RewindChannel> live(joinNamed, 1, channel);
  for (std::vector<char>& packet : venue.a.packets) {
    live.onPacket(packet.data(), packet.size(),
                  [&](char* message, const std::size_t size) { joiner.onPublic(message, size); });
  }
  CHECK(channel.requests.empty());

  // The witness who never left.
  NaiveBook devoted;
  for (std::vector<char>& packet : venue.a.packets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach([&](char* message, const std::size_t size) { devoted.onPublic(message, size); });
  }

  CHECK(joiner.canonical() == devoted.canonical());
  CHECK(!joiner.canonical().empty());
}
