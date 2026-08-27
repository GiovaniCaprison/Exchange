// The feed's central claims, held at once: the builder's book equals the matcher's own ladders
// order for order in priority order, which is the event stream proven sufficient to rebuild the
// state it describes; an outsider consuming only the public feed arrives at the same book, which
// is the public vocabulary proven sufficient too; and nothing on the feed can say whose,
// because no public message has a field that could.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <vector>

#include "exchange_protocol/PublicSessionState.h"
#include "exchange_protocol/SnapshotComplete.h"
#include "feedtest.hpp"
#include "glimpse.hpp"
#include "ladder.hpp"
#include "ranges.hpp"

using namespace exchange::marketdata;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;

namespace {

// The matcher's own book, walked best to worse, queues in order: the truth the builder is held
// to, read exactly the way the matcher's invariants suite reads it.
std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> engineCanonical(
    const exchange::matcher::Partition<CapturingRing>& partition, const std::uint32_t instrument) {
  std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> out;
  const auto& engine = partition.engine(instrument);
  const auto& book = engine.book();
  const auto& slab = engine.slab();
  for (std::int32_t side = 0; side <= 1; side++) {
    const auto& ladder = book.ladderOf(side);
    for (std::int32_t rank = ladder.best(); rank != exchange::matcher::Ladder::EMPTY;
         rank = ladder.occupiedFrom(rank + 1)) {
      for (std::int32_t slot = ladder.headAt(rank); slot != 0;
           slot = static_cast<std::int32_t>(slab.hot(slot).next)) {
        if (slab.hot(slot).displayed > 0) {
          out.emplace_back(slab.hot(slot).id, book.priceOfRank(side, rank),
                           slab.hot(slot).displayed);
        }
      }
    }
  }
  return out;
}

// The builder's book through its own snapshot, which serves it in the same priority order.
std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> snapshotCanonical(
    const WiredBuilder& builder, const std::uint64_t joinAt) {
  std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> out;
  snapshot(builder, joinAt, [&](char* packet, const std::size_t length) {
    common::ranges::Reader reader(packet, length);
    reader.forEach([&](char* message, const std::size_t size) {
      exchange::protocol::MessageHeader wrap;
      wrap.wrap(message, 0, 0, size);
      if (wrap.templateId() == exchange::protocol::PublicOrderAdded::sbeTemplateId()) {
        exchange::protocol::PublicOrderAdded added;
        added.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            size);
        out.emplace_back(added.orderId(), added.price(), added.quantity());
      }
    });
  });
  return out;
}

}  // namespace

TEST_CASE("the builder equals the matcher's ladders, and an outsider equals the builder") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(71, 6000);
  Venue venue;
  venue.play(flow, 0, flow.size());

  // The builder against the engine, order for order, best to worse, queues in order.
  const auto fromEngine =
      engineCanonical(venue.partition, exchange::matcher::test::FLOW_INSTRUMENT);
  const auto fromBuilder = snapshotCanonical(venue.builder, venue.publisher.next());
  CHECK(!fromEngine.empty());
  CHECK(fromEngine == fromBuilder);

  // The outsider against the builder: only the public packets, through the consumer's door.
  NaiveBook outsider;
  for (std::vector<char>& packet : venue.a.packets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach(
        [&](char* message, const std::size_t size) { outsider.onPublic(message, size); });
  }
  CHECK(outsider.canonical() == fromBuilder);

  // The privacy claim, structurally: only the public vocabulary rides the feed.
  for (std::vector<char>& packet : venue.a.packets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach([&](char* message, const std::size_t size) {
      exchange::protocol::MessageHeader wrap;
      wrap.wrap(message, 0, 0, size);
      CHECK(wrap.templateId() >= 40);
      CHECK(wrap.templateId() <= 47);
    });
  }

  // The B feed is the A feed, byte for byte.
  CHECK(venue.a.packets == venue.b.packets);
}
