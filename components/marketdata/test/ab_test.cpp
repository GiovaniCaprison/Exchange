// A and B, and what loss costs: a consumer feeding both streams into one packet source pays
// nothing for a packet lost on one feed, because the twin carried it and the source skips what
// it has already delivered; only a range both feeds lost reaches the retransmission server, and
// the count of rewind requests is held to exactly that overlap.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "feedtest.hpp"
#include "ranges.hpp"
#include "stream.hpp"
#include "submission.hpp"

using namespace exchange::marketdata;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;
using exchange::sequencer::test::RewindChannel;

TEST_CASE("what one feed drops the other carries, and only shared loss is rewound") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(79, 4000);
  Venue venue;
  venue.play(flow, 0, flow.size());
  REQUIRE(venue.a.packets.size() > 12);

  // A loses every third packet, B loses every fourth: some packets vanish from both. The two
  // feeds carry the same packet at the same moment, so arrival interleaves by original index,
  // which is what makes the twin's copy arrive before a gap could be declared.
  std::size_t sharedLoss = 0;
  NaiveBook consumer;
  RewindChannel channel;
  common::stream::PacketSource<RewindChannel> source(1, 1, channel);
  std::size_t rewinds = 0;
  const auto handler = [&](char* message, const std::size_t size) {
    consumer.onPublic(message, size);
  };
  for (std::size_t at = 0; at < venue.a.packets.size(); at++) {
    const bool aDrops = at % 3 == 2;
    const bool bDrops = at % 4 == 3;
    if (aDrops && bDrops) {
      sharedLoss++;
    }
    if (!aDrops) {
      std::vector<char> copy = venue.a.packets[at];
      source.onPacket(copy.data(), copy.size(), handler);
    }
    if (!bDrops) {
      std::vector<char> copy = venue.b.packets[at];
      source.onPacket(copy.data(), copy.size(), handler);
    }
    while (!channel.requests.empty()) {
      const RewindChannel::Request request = channel.requests.front();
      channel.requests.erase(channel.requests.begin());
      rewinds++;
      venue.publisher.serveRewind(request.firstSequence, request.count,
                                  [&](char* packet, const std::size_t length) {
                                    source.onPacket(packet, length, handler);
                                  });
    }
  }
  REQUIRE(sharedLoss > 0);

  // The stream is whole, and the rewinder was only bothered for what both feeds lost.
  NaiveBook devoted;
  for (std::vector<char>& packet : venue.a.packets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach([&](char* message, const std::size_t size) { devoted.onPublic(message, size); });
  }
  CHECK(consumer.canonical() == devoted.canonical());
  CHECK(rewinds <= sharedLoss);
  CHECK(rewinds > 0);
}
