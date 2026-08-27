// The bots, seated at the real venue: a market maker quoting around an inventory-skewed
// reservation, noise takers arriving at random, a momentum taker chasing runs of prints. The
// flagship is determinism over people: the same seeded day driven twice through the whole room
// journals identical bytes, because every participant is a pure function of the stream and its
// seeds. The dealer's law is held too: the maker leans its quotes away from inventory and stops
// bidding at its limit.

#include "bots.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "room.hpp"

using namespace exchange::ecosystem;
using namespace exchange::ecosystem::test;
namespace sbe = exchange::protocol;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-bots-" + name);
}

std::vector<char> fileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

struct DayFacts {
  std::uint64_t makerQuotes = 0;
  std::uint64_t makerMoves = 0;
  std::uint64_t noiseFired = 0;
  std::uint64_t executions = 0;
  std::int64_t worstInventory = 0;
};

// One seeded session: the maker deals, the crowd arrives, the chaser chases, and every fact the
// day produced comes back for the caller to judge.
DayFacts oneDay(const std::string& journalPath) {
  Room room(journalPath);
  Seat& dealer = room.seat(7, 42);
  Seat& crowdA = room.seat(8, 43);
  Seat& crowdB = room.seat(9, 44);
  Seat& chaser = room.seat(10, 45);
  room.open(1);

  Maker<Feed, Entry> maker(dealer.feed, dealer.entry, {});
  NoiseTaker<Feed, Entry> noiseA(crowdA.feed, crowdA.entry, {}, 11);
  NoiseTaker<Feed, Entry> noiseB(crowdB.feed, crowdB.entry, {}, 22);
  MomentumTaker<Feed, Entry> momentum(chaser.feed, chaser.entry, {});

  DayFacts facts;
  for (int at = 0; at < 600; at++) {
    room.tick(1'000'000, [&] {
      maker.onTick();
      noiseA.onTick();
      noiseB.onTick();
      momentum.onTick();
    });
    const std::int64_t inventory = std::llabs(dealer.entry.positionOf(1));
    if (inventory > facts.worstInventory) {
      facts.worstInventory = inventory;
    }
  }
  facts.makerQuotes = maker.quotesPlaced();
  facts.makerMoves = maker.quotesMoved();
  facts.noiseFired = noiseA.fired() + noiseB.fired();
  facts.executions = dealer.entry.executions();
  return facts;
}

}  // namespace

TEST_CASE("the same seeded day, twice, journals identical bytes, participants included") {
  const std::filesystem::path first = scratch("day-one.exj");
  const std::filesystem::path second = scratch("day-two.exj");
  const DayFacts one = oneDay(first.string());
  const DayFacts two = oneDay(second.string());

  // The day was a market: the dealer quoted and re-quoted, the crowd hit it, trades printed.
  CHECK(one.makerQuotes > 0);
  CHECK(one.makerMoves > 0);
  CHECK(one.noiseFired > 20);
  CHECK(one.executions > 20);

  // The dealer's inventory never ran past its limit plus one standing quote's fill.
  Maker<Feed, Entry>::Config dealt;
  CHECK(one.worstInventory <= dealt.inventoryLimit + dealt.quoteSize);

  const std::vector<char> bytes = fileBytes(first.string());
  CHECK(!bytes.empty());
  CHECK(bytes == fileBytes(second.string()));
  std::filesystem::remove(first);
  std::filesystem::remove(second);
}

TEST_CASE("the maker leans away from inventory and stops bidding at its limit") {
  const std::filesystem::path journal = scratch("lean.exj");
  Room room(journal.string());
  Seat& dealer = room.seat(7, 42);
  Seat& seller = room.seat(8, 43);
  room.open(1);

  Maker<Feed, Entry>::Config config;
  Maker<Feed, Entry> maker(dealer.feed, dealer.entry, config);
  room.tick(1'000'000, [&] { maker.onTick(); });
  const std::int64_t restingBid = dealer.feed.touchOf(1).bidPrice;
  REQUIRE(dealer.feed.touchOf(1).bidQuantity > 0);

  // The crowd sells into the dealer until it will not bid any more. A fill lands every few
  // ticks, since the quote, the hit and the requote each take a beat of the room.
  for (int hits = 0; hits < 80; hits++) {
    room.tick(1'000'000, [&] {
      const Touch touch = seller.feed.touchOf(1);
      if (touch.bidQuantity > 0) {
        seller.entry.newOrder(1, sbe::Side::SELL, sbe::Pricing::LIMIT,
                              sbe::TimeInForce::IMMEDIATE_OR_CANCEL, touch.bidPrice,
                              config.quoteSize);
      }
      maker.onTick();
    });
  }

  // Long inventory pushed the reservation down, so the dealer's bid leaned below where it began,
  // and at the limit the dealer stopped bidding entirely.
  CHECK(dealer.entry.positionOf(1) >= config.inventoryLimit);
  CHECK(dealer.entry.positionOf(1) <= config.inventoryLimit + config.quoteSize);
  const Touch after = dealer.feed.touchOf(1);
  CHECK(after.bidQuantity == 0);
  CHECK(dealer.feed.lastTradeOf(1).price < restingBid);
  std::filesystem::remove(journal);
}
