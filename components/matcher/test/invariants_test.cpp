// What has to be true of the matcher's structures after any command sequence at all. The failure
// these hunt is drift: every operation locally correct and the structure slowly wrong, which no
// fixture catches because nobody wrote it down. Generated flow reaches states nobody imagined, and
// after every command the caches are held to the queues they summarise (P-7): level totals to the
// orders under them, the bitmap and the cached best to the ladder, the name index to the queues,
// and the visible book a consumer rebuilds from the events to the engine's own.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::matcher;
using namespace exchange::matcher::test;

namespace {

constexpr std::uint32_t INSTRUMENT = 1;

// Deterministic and fast, which is all a state-space generator owes anybody (P-14 owns fidelity
// questions; this flow exists to reach states, and the measurement harness has its own).
class Random {
 public:
  explicit Random(const std::uint64_t seed) : state_(seed * 2685821657736338717ULL + 1) {}

  std::uint64_t next() {
    state_ ^= state_ >> 12;
    state_ ^= state_ << 25;
    state_ ^= state_ >> 27;
    return state_ * 2685821657736338717ULL;
  }

  std::int64_t below(const std::int64_t bound) {
    return static_cast<std::int64_t>(next() % static_cast<std::uint64_t>(bound));
  }

 private:
  std::uint64_t state_;
};

struct Generated {
  CommandWriter writer;
  Random random;
  std::vector<std::uint64_t> live;
  std::uint64_t nextClient = 1;

  explicit Generated(const std::uint64_t seed) : random(seed) {}

  CommandWriter::Framed next(const bool auctions, const std::uint64_t at) {
    if (auctions && at % 2000 == 0) {
      return writer.session(INSTRUMENT, at % 4000 == 0 ? sbe::SessionState::OPENING_AUCTION
                                                       : sbe::SessionState::CONTINUOUS);
    }
    const std::int64_t roll = random.below(100);
    if (roll < 40 && !live.empty()) {
      const std::size_t pick =
          static_cast<std::size_t>(random.below(static_cast<std::int64_t>(live.size())));
      const std::uint64_t client = live[pick];
      live[pick] = live.back();
      live.pop_back();
      return writer.cancel(INSTRUMENT, client, 1 + static_cast<std::uint32_t>(client % 5));
    }
    if (roll < 50 && !live.empty()) {
      const std::uint64_t client =
          live[static_cast<std::size_t>(random.below(static_cast<std::int64_t>(live.size())))];
      const std::int64_t price = 100000 + 5 * random.below(60) - 5 * random.below(60);
      return writer.replace(INSTRUMENT, client, 1 + static_cast<std::uint32_t>(client % 5),
                            1 + random.below(60), price);
    }
    if (roll < 51 && at > 500) {
      return writer.massCancel(INSTRUMENT, 0, 1 + static_cast<std::uint32_t>(random.below(5)));
    }
    const std::uint64_t client = nextClient++;
    const bool aggressive = roll >= 90;
    const std::int64_t offset = 5 * random.below(aggressive ? 3 : 40);
    const bool buy = random.below(2) == 0;
    const std::int64_t price =
        buy ? 100000 - (aggressive ? -offset : offset) : 100000 + (aggressive ? -offset : offset);
    const bool market = aggressive && random.below(4) == 0;
    const bool ioc = aggressive || random.below(20) == 0;
    const std::int64_t quantity = 1 + random.below(40);
    const std::int64_t display = random.below(15) == 0 ? 1 + random.below(quantity) : 0;
    const std::int64_t trigger =
        random.below(50) == 0 ? 100000 + 5 * random.below(30) - 5 * random.below(30) : 0;
    const std::uint64_t smp =
        random.below(10) == 0 ? 1 + static_cast<std::uint64_t>(random.below(5)) : 0;
    if (!ioc && !market) {
      live.push_back(client);
    }
    return writer.newOrder(
        INSTRUMENT, client, 1 + static_cast<std::uint32_t>(client % 5),
        buy ? sbe::Side::BUY : sbe::Side::SELL, market ? sbe::Pricing::MARKET : sbe::Pricing::LIMIT,
        ioc ? sbe::TimeInForce::IMMEDIATE_OR_CANCEL : sbe::TimeInForce::GOOD_TILL_CANCEL, false,
        market ? 0 : std::max<std::int64_t>(price, 5), quantity, 0, display,
        trigger == 0 ? 0 : std::max<std::int64_t>(trigger, 5), smp);
  }
};

// The visible book a consumer holds, rebuilt from events alone: id, side, price, displayed, in
// arrival order, where a replenishment moves the order to the back the way the queue did.
struct ConsumerBook {
  struct Entry {
    std::uint64_t id;
    std::int32_t side;
    std::int64_t price;
    std::int64_t quantity;
    bool operator==(const Entry&) const = default;
  };
  std::vector<Entry> entries;

  void apply(const EventView& event) {
    if (event.templateId == sbe::OrderRested::sbeTemplateId()) {
      erase(event.orderId);
      entries.push_back({event.orderId, event.side, event.price, event.quantity});
      return;
    }
    if (event.templateId == sbe::OrderReduced::sbeTemplateId()) {
      for (Entry& entry : entries) {
        if (entry.id == event.orderId) {
          entry.quantity = event.quantity;
        }
      }
      return;
    }
    if (event.templateId == sbe::OrderRemoved::sbeTemplateId()) {
      erase(event.orderId);
      return;
    }
    if (event.templateId == sbe::OrderExecuted::sbeTemplateId()) {
      // An execution reduces whichever of its two orders this book holds: the resting order
      // alone in continuous trading, and both in an uncrossing, where neither side aggressed.
      reduceBy(event.restingOrderId, event.quantity);
      reduceBy(event.aggressorOrderId, event.quantity);
    }
  }

  void reduceBy(const std::uint64_t id, const std::int64_t quantity) {
    for (std::size_t at = 0; at < entries.size(); at++) {
      if (entries[at].id == id) {
        entries[at].quantity -= quantity;
        if (entries[at].quantity == 0) {
          entries.erase(entries.begin() + static_cast<long>(at));
        }
        return;
      }
    }
  }

  void erase(const std::uint64_t id) {
    for (std::size_t at = 0; at < entries.size(); at++) {
      if (entries[at].id == id) {
        entries.erase(entries.begin() + static_cast<long>(at));
        return;
      }
    }
  }
};

void check(const Partition<CapturingRing>& partition, const ConsumerBook& rebuilt,
           const std::uint64_t command) {
  const Engine<CapturingRing>& engine = partition.engine(INSTRUMENT);
  const Book& book = engine.book();
  const Slab& slab = engine.slab();
  const std::string where = "after command " + std::to_string(command);

  std::size_t queued = 0;
  std::vector<ConsumerBook::Entry> visible;
  for (std::int32_t side = 0; side <= 1; side++) {
    const Ladder& ladder = book.ladderOf(side);
    // The cached best agrees with a fresh search of the summaries.
    INFO(where << ": side " << side);
    CHECK(ladder.best() == ladder.occupiedFrom(0));
    for (std::int32_t rank = ladder.best(); rank != Ladder::EMPTY;
         rank = ladder.occupiedFrom(rank + 1)) {
      // Every rank the bitmap names holds a queue, read the same forwards and backwards, and the
      // cached totals equal the sums of the orders under them (P-7).
      std::int64_t shown = 0;
      std::int64_t left = 0;
      std::vector<std::int32_t> forward;
      for (std::int32_t slot = ladder.headAt(rank); slot != 0;
           slot = static_cast<std::int32_t>(slab.hot(slot).next)) {
        forward.push_back(slot);
        shown += slab.hot(slot).displayed;
        left += slab.hot(slot).remaining;
        CHECK(std::cmp_equal(slab.cold(slot).side, side));
        CHECK(book.rankOf(side, slab.hot(slot).tick) == rank);
        queued++;
      }
      std::vector<std::int32_t> backward;
      for (std::int32_t slot = ladder.tailAt(rank); slot != 0;
           slot = static_cast<std::int32_t>(slab.hot(slot).previous)) {
        backward.push_back(slot);
      }
      std::reverse(backward.begin(), backward.end());
      INFO(where << ": rank " << rank << " on side " << side);
      CHECK(!forward.empty());
      CHECK(backward == forward);
      CHECK(ladder.displayedAt(rank) == shown);
      CHECK(ladder.remainingAt(rank) == left);
    }
  }

  // The index holds exactly what the queues hold, every indexed order's bit is set, ids are
  // unique, and no order is in the book and the trigger book at once.
  const std::vector<std::int32_t> resting = book.restingSlots();
  INFO(where);
  CHECK(resting.size() == queued);
  std::set<std::uint64_t> ids;
  for (const std::int32_t slot : resting) {
    CHECK(book.ladderOf(slab.cold(slot).side)
              .occupied(book.rankOf(slab.cold(slot).side, slab.hot(slot).tick)));
    CHECK(ids.insert(slab.hot(slot).id).second);
  }
  for (const std::int32_t stop : engine.waitingStops()) {
    CHECK(ids.insert(slab.hot(stop).id).second);
  }

  // The book a consumer holds is the book the engine holds.
  std::vector<std::int32_t> byArrival = resting;
  std::sort(byArrival.begin(), byArrival.end(),
            [&slab](const std::int32_t left, const std::int32_t right) {
              return slab.hot(left).arrival < slab.hot(right).arrival;
            });
  visible.reserve(visible.size() + byArrival.size());
  for (const std::int32_t slot : byArrival) {
    visible.push_back({slab.hot(slot).id, slab.cold(slot).side,
                       book.priceOfTick(slab.hot(slot).tick), slab.hot(slot).displayed});
  }
  CHECK(visible == rebuilt.entries);
}

// The whole ladder, tick by tick, once the flow is over: an unoccupied level is empty, unlinked
// and holds nothing, so a cleared bit never lies either.
void sweep(const Partition<CapturingRing>& partition) {
  const Book& book = partition.engine(INSTRUMENT).book();
  for (std::int32_t side = 0; side <= 1; side++) {
    const Ladder& ladder = book.ladderOf(side);
    for (std::int32_t rank = 0; rank < ladder.rankCount(); rank++) {
      if (ladder.occupied(rank)) {
        CHECK(ladder.headAt(rank) != 0);
      } else {
        CHECK(ladder.headAt(rank) == 0);
        CHECK(ladder.displayedAt(rank) == 0);
        CHECK(ladder.remainingAt(rank) == 0);
      }
    }
  }
}

void drive(const std::uint64_t seed, const bool auctions) {
  CapturingRing ring;
  Partition<CapturingRing> partition(ring);
  Generated generated(seed);
  ConsumerBook rebuilt;
  // Only the bytes each command appended are decoded, so the replay stays linear in the flow.
  std::size_t seenBytes = 0;

  CommandWriter::Framed definition =
      generated.writer.instrument(INSTRUMENT, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false);
  partition.onCommand(definition.bytes.data(), 0, definition.bytes.size());

  CommandWriter::Framed open = generated.writer.session(INSTRUMENT, sbe::SessionState::CONTINUOUS);
  partition.onCommand(open.bytes.data(), 0, open.bytes.size());

  for (std::uint64_t command = 1; command <= 6000; command++) {
    CommandWriter::Framed framed = generated.next(auctions, command);
    partition.onCommand(framed.bytes.data(), 0, framed.bytes.size());
    const std::vector<char>& bytes = ring.captured();
    const std::vector<char> fresh(bytes.begin() + static_cast<long>(seenBytes), bytes.end());
    seenBytes = bytes.size();
    for (const EventView& event : readEvents(fresh)) {
      rebuilt.apply(event);
    }
    check(partition, rebuilt, command);
  }
  sweep(partition);
}

}  // namespace

TEST_CASE("nothing drifts across generated continuous flow") {
  drive(1, false);
  drive(20260826, false);
}

TEST_CASE("nothing drifts across generated flow with call phases") { drive(7, true); }
