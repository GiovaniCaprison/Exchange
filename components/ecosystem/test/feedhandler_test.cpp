// The feed handler, held to the outsider's reference: fed the same public feed, its book is the
// naive book's book and its touch is that book's touch, order for order; loss on either twin
// costs it nothing because the packet source repairs before delivering; and a late joiner that
// takes the snapshot first ends the day equal to the consumer that heard everything.

#include "feedhandler.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <tuple>
#include <vector>

#include "feedtest.hpp"
#include "glimpse.hpp"
#include "ranges.hpp"
#include "submission.hpp"

using namespace exchange::ecosystem;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
using exchange::sequencer::test::RewindChannel;
namespace common = exchange::common;

namespace {

using Handler = FeedHandler<RewindChannel>;

// The same canonical form NaiveBook speaks, so equality compares like with like.
std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> canonicalOf(
    const Handler& handler) {
  std::vector<Handler::PublicOrder> rows;
  handler.forEachOrder([&](const Handler::PublicOrder& order) { rows.push_back(order); });
  std::sort(rows.begin(), rows.end(),
            [](const Handler::PublicOrder& a, const Handler::PublicOrder& b) {
              if (a.instrumentId != b.instrumentId) {
                return a.instrumentId < b.instrumentId;
              }
              if (a.side != b.side) {
                return a.side < b.side;
              }
              if (a.price != b.price) {
                return a.side == 0 ? a.price > b.price : a.price < b.price;
              }
              return a.arrival < b.arrival;
            });
  std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> out;
  out.reserve(rows.size());
  for (const Handler::PublicOrder& row : rows) {
    out.emplace_back(row.id, row.price, row.quantity);
  }
  return out;
}

// The touch the naive book implies, computed the slow obvious way.
Touch touchFrom(const NaiveBook& book, const std::uint32_t instrumentId) {
  Touch touch;
  for (const auto& [id, order] : book.orders) {
    if (order.instrument != instrumentId || order.quantity <= 0) {
      continue;
    }
    if (order.side == 0) {
      if (touch.bidQuantity == 0 || order.price > touch.bidPrice) {
        touch.bidPrice = order.price;
        touch.bidQuantity = order.quantity;
      } else if (order.price == touch.bidPrice) {
        touch.bidQuantity += order.quantity;
      }
    } else {
      if (touch.askQuantity == 0 || order.price < touch.askPrice) {
        touch.askPrice = order.price;
        touch.askQuantity = order.quantity;
      } else if (order.price == touch.askPrice) {
        touch.askQuantity += order.quantity;
      }
    }
  }
  return touch;
}

void feedAll(Handler& handler, std::vector<std::vector<char>>& packets) {
  for (std::vector<char>& packet : packets) {
    std::vector<char> copy = packet;
    handler.onPacket(copy.data(), copy.size());
  }
}

}  // namespace

TEST_CASE("the feed handler's book is the outsider's book, and its touch is that book's touch") {
  const auto flow = generatedFlow(101, 6000);
  Venue venue;
  venue.play(flow, 0, flow.size());

  RewindChannel channel;
  Handler handler(channel);
  feedAll(handler, venue.a.packets);

  NaiveBook reference;
  for (std::vector<char>& packet : venue.a.packets) {
    common::ranges::Reader reader(packet.data(), packet.size());
    reader.forEach(
        [&](char* message, const std::size_t size) { reference.onPublic(message, size); });
  }

  CHECK(canonicalOf(handler) == reference.canonical());
  CHECK(!handler.instruments().empty());
  for (const std::uint32_t instrument : handler.instruments()) {
    const Touch seen = handler.touchOf(instrument);
    const Touch implied = touchFrom(reference, instrument);
    CHECK(seen.bidPrice == implied.bidPrice);
    CHECK(seen.bidQuantity == implied.bidQuantity);
    CHECK(seen.askPrice == implied.askPrice);
    CHECK(seen.askQuantity == implied.askQuantity);
  }
  CHECK(channel.requests.empty());
}

TEST_CASE("loss on either twin costs nothing, and repair leaves the touch whole") {
  const auto flow = generatedFlow(79, 4000);
  Venue venue;
  venue.play(flow, 0, flow.size());
  REQUIRE(venue.a.packets.size() > 12);

  RewindChannel channel;
  Handler lossy(channel);
  for (std::size_t at = 0; at < venue.a.packets.size(); at++) {
    if (at % 3 != 2) {
      std::vector<char> copy = venue.a.packets[at];
      lossy.onPacket(copy.data(), copy.size());
    }
    if (at % 4 != 3) {
      std::vector<char> copy = venue.b.packets[at];
      lossy.onPacket(copy.data(), copy.size());
    }
    while (!channel.requests.empty()) {
      const RewindChannel::Request request = channel.requests.front();
      channel.requests.erase(channel.requests.begin());
      venue.publisher.serveRewind(
          request.firstSequence, request.count,
          [&](char* packet, const std::size_t length) { lossy.onPacket(packet, length); });
    }
  }

  RewindChannel devotedChannel;
  Handler devoted(devotedChannel);
  feedAll(devoted, venue.a.packets);

  CHECK(canonicalOf(lossy) == canonicalOf(devoted));
  for (const std::uint32_t instrument : devoted.instruments()) {
    CHECK(lossy.touchOf(instrument).bidPrice == devoted.touchOf(instrument).bidPrice);
    CHECK(lossy.touchOf(instrument).askPrice == devoted.touchOf(instrument).askPrice);
    CHECK(lossy.lastTradeOf(instrument).price == devoted.lastTradeOf(instrument).price);
  }
}

TEST_CASE("a late joiner via the snapshot ends the day equal to the devoted consumer") {
  const auto flow = generatedFlow(31, 5000);
  Venue venue;
  venue.play(flow, 0, flow.size() / 2);

  // The glimpse: the book as ranges, then the word naming where the live feed continues.
  std::vector<std::vector<char>> snapshot;
  exchange::marketdata::snapshot(
      venue.builder, venue.publisher.next(),
      [&](char* bytes, const std::size_t length) { snapshot.emplace_back(bytes, bytes + length); });

  RewindChannel channel;
  Handler joiner(channel, true);
  CHECK(!joiner.joined());
  for (std::vector<char>& range : snapshot) {
    joiner.onSnapshotRange(range.data(), range.size());
  }
  CHECK(joiner.joined());

  venue.play(flow, flow.size() / 2, flow.size());
  // The joiner is fed the whole feed; everything the snapshot already covers is dropped by
  // sequence, which is exactly what joining at the named point means.
  feedAll(joiner, venue.a.packets);

  RewindChannel devotedChannel;
  Handler devoted(devotedChannel);
  feedAll(devoted, venue.a.packets);

  CHECK(canonicalOf(joiner) == canonicalOf(devoted));
  for (const std::uint32_t instrument : devoted.instruments()) {
    CHECK(joiner.touchOf(instrument).bidPrice == devoted.touchOf(instrument).bidPrice);
    CHECK(joiner.touchOf(instrument).bidQuantity == devoted.touchOf(instrument).bidQuantity);
    CHECK(joiner.touchOf(instrument).askPrice == devoted.touchOf(instrument).askPrice);
    CHECK(joiner.touchOf(instrument).askQuantity == devoted.touchOf(instrument).askQuantity);
    CHECK(joiner.stateOf(instrument) == devoted.stateOf(instrument));
  }
}
