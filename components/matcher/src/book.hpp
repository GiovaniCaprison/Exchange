// One instrument's book: a flat ladder per side over the slab's queues, and a name index over one
// interleaved array. The instrument definition is what licenses the ladder: every valid price is a
// multiple of the tick inside the static band, so prices map to a dense tick index, the tick maps
// to a side's rank, and finding a level is an array read. Validation happens before any price
// reaches here (P-9), so the mapping never checks its input.
//
// The name index maps the id a cancel carries (participant and client order id, exact) to a slot:
// open addressing over one interleaved array, two words per entry so a probe touches one cache
// line, deletion by backward shift so the table's occupancy is its contents and the steady state
// never rehashes (P-6).

#pragma once

#include <cstdint>
#include <vector>

#include "ladder.hpp"
#include "slab.hpp"

namespace exchange::matcher {

class Book {
 public:
  Book(Slab& slab, const std::int64_t tickSize, const std::int64_t baseTick,
       const std::int32_t rankCount)
      : slab_(slab),
        bids_(slab, rankCount),
        asks_(slab, rankCount),
        tickSize_(tickSize),
        baseTick_(baseTick),
        maxRank_(rankCount - 1) {
    allocateNames(1 << 16);
  }

  // Geometry --------------------------------------------------------------------------------

  std::int64_t priceOfTick(const std::int32_t tick) const { return (tick + baseTick_) * tickSize_; }

  // The tick of a price validation already admitted (P-9); never checked here.
  std::int32_t tickOfPrice(const std::int64_t price) const {
    return static_cast<std::int32_t>(price / tickSize_ - baseTick_);
  }

  // A side's view of a tick: bids rank best-first by reversing, asks are already ascending.
  std::int32_t rankOf(const std::int32_t side, const std::int32_t tick) const {
    return side == 0 ? maxRank_ - tick : tick;
  }

  std::int32_t tickOfRank(const std::int32_t side, const std::int32_t rank) const {
    return side == 0 ? maxRank_ - rank : rank;
  }

  std::int64_t priceOfRank(const std::int32_t side, const std::int32_t rank) const {
    return priceOfTick(tickOfRank(side, rank));
  }

  // Every rank a market order can reach, which is all of them and below Ladder::EMPTY.
  std::int32_t marketLimit() const { return maxRank_; }

  // The worst rank on the side still willing at the price: at it, or better from its own side.
  std::int32_t willingLimitRank(const std::int32_t side, const std::int64_t price) const {
    return rankOf(side, tickOfPrice(price));
  }

  // The venue's questions -------------------------------------------------------------------

  // The order a taker reaches next: the front of the best crossing level, where the crossing test
  // is one comparison because the limit arrived as a rank in the resting side's own space.
  std::int32_t nextToTake(const std::int32_t takerSide, const std::int32_t limitRank) const {
    const Ladder& resting = ladder(takerSide ^ 1);
    const std::int32_t best = resting.best();
    return best > limitRank ? 0 : resting.headAt(best);
  }

  void add(const std::int32_t side, const std::int32_t slot) {
    ladder(side).append(rankOf(side, slab_.hot(slot).tick), slot);
    namePut(slot);
  }

  void remove(const std::int32_t side, const std::int32_t slot) {
    ladder(side).unlink(rankOf(side, slab_.hot(slot).tick), slot);
    nameRemove(slab_.cold(slot).participantId, slab_.cold(slot).clientOrderId);
  }

  // The order's quantities changed in place, so the level's totals follow (P-7).
  void quantitiesChanged(const std::int32_t side, const std::int32_t slot,
                         const std::int64_t displayedDelta, const std::int64_t remainingDelta) {
    ladder(side).adjust(rankOf(side, slab_.hot(slot).tick), displayedDelta, remainingDelta);
  }

  void requeued(const std::int32_t side, const std::int32_t slot, const std::int64_t displayedDelta,
                const std::int64_t remainingDelta) {
    ladder(side).requeue(rankOf(side, slab_.hot(slot).tick), slot, displayedDelta, remainingDelta);
  }

  // The slot resting under the name, or zero.
  std::int32_t named(const std::uint32_t participantId, const std::uint64_t clientOrderId) const {
    std::size_t at = homeOf(participantId, clientOrderId);
    while (slotAt(at) != 0) {
      if (names_[at << 1] == clientOrderId &&
          static_cast<std::uint32_t>(names_[(at << 1) + 1]) == participantId) {
        return slotAt(at);
      }
      at = (at + 1) & nameMask_;
    }
    return 0;
  }

  // Every resting order for one participant appended to the caller's space.
  void of(const std::uint32_t participantId, std::vector<std::int32_t>& into) const {
    for (std::size_t at = 0; at <= nameMask_; at++) {
      const std::uint64_t entry = names_[(at << 1) + 1];
      if (static_cast<std::int32_t>(entry >> 32) != 0 &&
          static_cast<std::uint32_t>(entry) == participantId) {
        into.push_back(static_cast<std::int32_t>(entry >> 32));
      }
    }
  }

  void all(std::vector<std::int32_t>& into) const {
    for (std::size_t at = 0; at <= nameMask_; at++) {
      const std::uint64_t entry = names_[(at << 1) + 1];
      if (static_cast<std::int32_t>(entry >> 32) != 0) {
        into.push_back(static_cast<std::int32_t>(entry >> 32));
      }
    }
  }

  // How much a taker could fill, summed over crossing levels only. With no self match id in play
  // the cached totals answer without touching an order; with one, the queues are walked, since
  // the exclusion is per order.
  std::int64_t fillable(const std::int32_t takerSide, const std::int32_t limitRank,
                        const std::uint64_t smpId) const {
    const Ladder& resting = ladder(takerSide ^ 1);
    std::int64_t total = 0;
    for (std::int32_t rank = resting.best(); rank <= limitRank;
         rank = resting.occupiedFrom(rank + 1)) {
      if (smpId == 0) {
        total += resting.remainingAt(rank);
      } else {
        for (std::int32_t slot = resting.headAt(rank); slot != 0;
             slot = static_cast<std::int32_t>(slab_.hot(slot).next)) {
          if (slab_.hot(slot).smpId != smpId) {
            total += slab_.hot(slot).remaining;
          }
        }
      }
    }
    return total;
  }

  // Walks for the auction and the uncrossing ------------------------------------------------

  std::int32_t firstRank(const std::int32_t side) const { return ladder(side).best(); }

  std::int32_t rankAfter(const std::int32_t side, const std::int32_t rank) const {
    return ladder(side).occupiedFrom(rank + 1);
  }

  std::int32_t headAtRank(const std::int32_t side, const std::int32_t rank) const {
    return ladder(side).headAt(rank);
  }

  std::int64_t remainingAtRank(const std::int32_t side, const std::int32_t rank) const {
    return ladder(side).remainingAt(rank);
  }

  // Views for the tests, allocation-free only where the hot path lives --------------------------

  const Ladder& ladderOf(const std::int32_t side) const { return ladder(side); }

  std::vector<std::int32_t> restingSlots() const {
    std::vector<std::int32_t> all;
    for (std::size_t at = 0; at <= nameMask_; at++) {
      if (slotAt(at) != 0) {
        all.push_back(slotAt(at));
      }
    }
    return all;
  }

  std::int32_t tickCount() const { return maxRank_ + 1; }

  void save(common::ByteSink& sink) const {
    bids_.save(sink);
    asks_.save(sink);
    sink.span(names_);
    sink.u64(nameMask_);
    sink.u64(nameCount_);
  }

  void restore(common::ByteSource& source) {
    bids_.restore(source);
    asks_.restore(source);
    source.span(names_);
    nameMask_ = source.u64();
    nameCount_ = source.u64();
  }

 private:
  Ladder& ladder(const std::int32_t side) { return side == 0 ? bids_ : asks_; }
  const Ladder& ladder(const std::int32_t side) const { return side == 0 ? bids_ : asks_; }

  // The name index ----------------------------------------------------------------------------

  std::int32_t slotAt(const std::size_t at) const {
    return static_cast<std::int32_t>(names_[(at << 1) + 1] >> 32);
  }

  void allocateNames(const std::size_t capacity) {
    names_.assign(capacity << 1, 0);
    nameMask_ = capacity - 1;
  }

  std::size_t homeOf(const std::uint32_t participantId, const std::uint64_t clientOrderId) const {
    std::uint64_t hash =
        clientOrderId * 0x9E3779B97F4A7C15ULL ^ participantId * 0xC2B2AE3D27D4EB4FULL;
    hash ^= hash >> 32;
    return static_cast<std::size_t>(hash) & nameMask_;
  }

  void namePut(const std::int32_t slot) {
    if ((nameCount_ + 1) * 2 > nameMask_ + 1) {
      grow();
    }
    const std::uint32_t participantId = slab_.cold(slot).participantId;
    const std::uint64_t clientOrderId = slab_.cold(slot).clientOrderId;
    std::size_t at = homeOf(participantId, clientOrderId);
    while (slotAt(at) != 0) {
      at = (at + 1) & nameMask_;
    }
    names_[at << 1] = clientOrderId;
    names_[(at << 1) + 1] = participantId | (static_cast<std::uint64_t>(slot) << 32);
    nameCount_++;
  }

  void nameRemove(const std::uint32_t participantId, const std::uint64_t clientOrderId) {
    std::size_t at = homeOf(participantId, clientOrderId);
    while (names_[at << 1] != clientOrderId ||
           static_cast<std::uint32_t>(names_[(at << 1) + 1]) != participantId) {
      at = (at + 1) & nameMask_;
    }
    nameCount_--;
    // Backward shift rather than a tombstone: the table's occupancy is its contents, so the load
    // never quietly climbs and the steady state never rehashes (P-6).
    std::size_t hole = at;
    std::size_t probe = (hole + 1) & nameMask_;
    while (slotAt(probe) != 0) {
      const std::size_t home =
          homeOf(static_cast<std::uint32_t>(names_[(probe << 1) + 1]), names_[probe << 1]);
      const bool reachable = ((probe - home) & nameMask_) >= ((probe - hole) & nameMask_);
      if (reachable) {
        names_[hole << 1] = names_[probe << 1];
        names_[(hole << 1) + 1] = names_[(probe << 1) + 1];
        hole = probe;
      }
      probe = (probe + 1) & nameMask_;
    }
    names_[(hole << 1) + 1] = 0;
  }

  void grow() {
    const std::vector<std::uint64_t> old = std::move(names_);
    allocateNames((nameMask_ + 1) * 2);
    nameCount_ = 0;
    for (std::size_t at = 0; at < old.size(); at += 2) {
      const std::int32_t slot = static_cast<std::int32_t>(old[at + 1] >> 32);
      if (slot != 0) {
        nameCount_++;
        std::size_t to = homeOf(static_cast<std::uint32_t>(old[at + 1]), old[at]);
        while (slotAt(to) != 0) {
          to = (to + 1) & nameMask_;
        }
        names_[to << 1] = old[at];
        names_[(to << 1) + 1] = old[at + 1];
      }
    }
  }

  Slab& slab_;
  Ladder bids_;
  Ladder asks_;

  std::int64_t tickSize_;
  std::int64_t baseTick_;
  std::int32_t maxRank_;

  // Client order id at entry << 1; participant in the low half beside it, slot above.
  std::vector<std::uint64_t> names_;

  std::size_t nameMask_ = 0;
  std::size_t nameCount_ = 0;
};

}  // namespace exchange::matcher
