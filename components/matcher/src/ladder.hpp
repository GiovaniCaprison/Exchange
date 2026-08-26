// One side's price levels as a flat array indexed by rank, with an occupancy bitmap above it. A
// rank is the side's own view of a tick: the ask ladder ranks ticks as they are and the bid ladder
// ranks them reversed, so rank zero is the best price on either side and every walk, every
// crossing test and every best-price search is the same arithmetic with no side branch.
//
// A level is two words: the queue's head and tail packed into one, and its cached displayed and
// remaining totals interleaved in another array so one line carries both (P-7 names the totals as
// policed caches). Best price discovery never scans: a three level bitmap says which ranks are
// occupied, sixty four ranks to a word and sixty four words to a summary bit, so the best rank is
// a cached int and its successor after a level empties is three trailing-zero counts away. The
// bits change exactly when a queue becomes empty or stops being empty, so the summary can never
// quietly disagree with the ladder, and the invariants suite holds it to that.

#pragma once

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <vector>

#include "slab.hpp"

namespace exchange::matcher {

class Ladder {
 public:
  // No occupied rank, above every reachable limit rank by construction.
  static constexpr std::int32_t EMPTY = std::numeric_limits<std::int32_t>::max();

  Ladder(Slab& slab, const std::int32_t ranks) : slab_(slab), ranks_(ranks) {
    queues_.assign(static_cast<std::size_t>(ranks), 0);
    totals_.assign(static_cast<std::size_t>(ranks) << 1, 0);
    bits0_.assign(words(static_cast<std::size_t>(ranks)), 0);
    bits1_.assign(words(bits0_.size()), 0);
    bits2_.assign(words(bits1_.size()), 0);
  }

  // The best occupied rank, or EMPTY, cached so the common question costs one read.
  std::int32_t best() const { return best_; }

  std::int32_t headAt(const std::int32_t rank) const {
    return static_cast<std::int32_t>(queues_[static_cast<std::size_t>(rank)] & 0xFFFFFFFF);
  }

  std::int32_t tailAt(const std::int32_t rank) const {
    return static_cast<std::int32_t>(queues_[static_cast<std::size_t>(rank)] >> 32);
  }

  std::int64_t displayedAt(const std::int32_t rank) const {
    return totals_[static_cast<std::size_t>(rank) << 1];
  }

  std::int64_t remainingAt(const std::int32_t rank) const {
    return totals_[(static_cast<std::size_t>(rank) << 1) + 1];
  }

  // The order's quantities changed in place, so the level's totals follow (P-7).
  void adjust(const std::int32_t rank, const std::int64_t displayedDelta,
              const std::int64_t remainingDelta) {
    totals_[static_cast<std::size_t>(rank) << 1] += displayedDelta;
    totals_[(static_cast<std::size_t>(rank) << 1) + 1] += remainingDelta;
  }

  // Joins the back of the queue at the rank, opening the level if nobody was there.
  void append(const std::int32_t rank, const std::int32_t slot) {
    const std::uint64_t queue = queues_[static_cast<std::size_t>(rank)];
    const std::int32_t tail = static_cast<std::int32_t>(queue >> 32);
    Slab::Hot& entry = slab_.hot(slot);
    entry.previous = static_cast<std::uint32_t>(tail);
    entry.next = 0;
    if (tail == 0) {
      queues_[static_cast<std::size_t>(rank)] = pack(slot, slot);
      set(rank);
      best_ = std::min(best_, rank);
    } else {
      slab_.hot(tail).next = static_cast<std::uint32_t>(slot);
      queues_[static_cast<std::size_t>(rank)] =
          pack(static_cast<std::int32_t>(queue & 0xFFFFFFFF), slot);
    }
    adjust(rank, entry.displayed, entry.remaining);
  }

  // Detaches the slot from its queue completely (P-8), closing the level when it empties: an empty
  // level does not survive, so the bitmap and the best cache never point at a price nobody is at.
  void unlink(const std::int32_t rank, const std::int32_t slot) {
    Slab::Hot& entry = slab_.hot(slot);
    adjust(rank, -entry.displayed, -entry.remaining);
    const std::int32_t previous = static_cast<std::int32_t>(entry.previous);
    const std::int32_t next = static_cast<std::int32_t>(entry.next);
    const std::uint64_t queue = queues_[static_cast<std::size_t>(rank)];
    std::int32_t head = static_cast<std::int32_t>(queue & 0xFFFFFFFF);
    std::int32_t tail = static_cast<std::int32_t>(queue >> 32);
    if (previous == 0) {
      head = next;
    } else {
      slab_.hot(previous).next = static_cast<std::uint32_t>(next);
    }
    if (next == 0) {
      tail = previous;
    } else {
      slab_.hot(next).previous = static_cast<std::uint32_t>(previous);
    }
    entry.previous = 0;
    entry.next = 0;
    queues_[static_cast<std::size_t>(rank)] = pack(head, tail);
    if (head == 0) {
      clear(rank);
      if (rank == best_) {
        best_ = occupiedFrom(rank);
      }
    }
  }

  // A replenished tranche joins the back of the queue at its price.
  void requeue(const std::int32_t rank, const std::int32_t slot, const std::int64_t displayedDelta,
               const std::int64_t remainingDelta) {
    Slab::Hot& entry = slab_.hot(slot);
    const std::int32_t previous = static_cast<std::int32_t>(entry.previous);
    const std::int32_t next = static_cast<std::int32_t>(entry.next);
    const std::uint64_t queue = queues_[static_cast<std::size_t>(rank)];
    const std::int32_t head = previous == 0 ? next : static_cast<std::int32_t>(queue & 0xFFFFFFFF);
    const std::int32_t tail = static_cast<std::int32_t>(queue >> 32);
    if (slot != tail) {
      if (previous == 0) {
        slab_.hot(next).previous = 0;
      } else {
        slab_.hot(previous).next = static_cast<std::uint32_t>(next);
        slab_.hot(next).previous = static_cast<std::uint32_t>(previous);
      }
      entry.previous = static_cast<std::uint32_t>(tail);
      entry.next = 0;
      slab_.hot(tail).next = static_cast<std::uint32_t>(slot);
      queues_[static_cast<std::size_t>(rank)] = pack(head, slot);
    }
    adjust(rank, displayedDelta, remainingDelta);
  }

  // The lowest occupied rank at or above the argument, or EMPTY: one masked word per level going
  // up, one trailing-zero count per level coming down, and no scan of the ladder anywhere.
  std::int32_t occupiedFrom(const std::int32_t rank) const {
    std::size_t word0 = static_cast<std::size_t>(rank) >> 6;
    if (word0 >= bits0_.size()) {
      return EMPTY;
    }
    const std::uint64_t low = bits0_[word0] & (~std::uint64_t{0} << (rank & 63));
    if (low != 0) {
      return static_cast<std::int32_t>((word0 << 6) +
                                       static_cast<std::size_t>(std::countr_zero(low)));
    }
    std::size_t word1 = word0 >> 6;
    std::uint64_t mid = above(bits1_[word1], word0 & 63);
    if (mid == 0) {
      std::size_t word2 = word1 >> 6;
      std::uint64_t high = above(bits2_[word2], word1 & 63);
      while (high == 0) {
        word2++;
        if (word2 == bits2_.size()) {
          return EMPTY;
        }
        high = bits2_[word2];
      }
      word1 = (word2 << 6) + static_cast<std::size_t>(std::countr_zero(high));
      mid = bits1_[word1];
    }
    word0 = (word1 << 6) + static_cast<std::size_t>(std::countr_zero(mid));
    return static_cast<std::int32_t>((word0 << 6) +
                                     static_cast<std::size_t>(std::countr_zero(bits0_[word0])));
  }

  // The bitmap's own answer for one rank, for the invariants that hold it to the ladder.
  bool occupied(const std::int32_t rank) const {
    return (bits0_[static_cast<std::size_t>(rank) >> 6] & (std::uint64_t{1} << (rank & 63))) != 0;
  }

  std::int32_t rankCount() const { return ranks_; }

  // The ladder's state is its arrays and its cached best; the geometry comes from construction.
  void save(common::ByteSink& sink) const {
    sink.span(queues_);
    sink.span(totals_);
    sink.span(bits0_);
    sink.span(bits1_);
    sink.span(bits2_);
    sink.i32(best_);
  }

  void restore(common::ByteSource& source) {
    source.span(queues_);
    source.span(totals_);
    source.span(bits0_);
    source.span(bits1_);
    source.span(bits2_);
    best_ = source.i32();
  }

 private:
  static std::size_t words(const std::size_t bits) { return (bits + 63) >> 6; }

  static std::uint64_t pack(const std::int32_t head, const std::int32_t tail) {
    return (static_cast<std::uint64_t>(tail) << 32) | static_cast<std::uint32_t>(head);
  }

  // The bits of the word strictly above the given bit, safe at the top where a shift wraps.
  static std::uint64_t above(const std::uint64_t word, const std::size_t bit) {
    return bit == 63 ? 0 : word & (~std::uint64_t{0} << (bit + 1));
  }

  void set(const std::int32_t rank) {
    const std::size_t word0 = static_cast<std::size_t>(rank) >> 6;
    const std::size_t word1 = word0 >> 6;
    bits0_[word0] |= std::uint64_t{1} << (rank & 63);
    bits1_[word1] |= std::uint64_t{1} << (word0 & 63);
    bits2_[word1 >> 6] |= std::uint64_t{1} << (word1 & 63);
  }

  void clear(const std::int32_t rank) {
    const std::size_t word0 = static_cast<std::size_t>(rank) >> 6;
    if ((bits0_[word0] &= ~(std::uint64_t{1} << (rank & 63))) != 0) {
      return;
    }
    const std::size_t word1 = word0 >> 6;
    if ((bits1_[word1] &= ~(std::uint64_t{1} << (word0 & 63))) != 0) {
      return;
    }
    bits2_[word1 >> 6] &= ~(std::uint64_t{1} << (word1 & 63));
  }

  Slab& slab_;
  std::int32_t ranks_;

  // Head in the low 32 bits, tail in the high 32, zero meaning nobody is at the price.
  std::vector<std::uint64_t> queues_;

  // Displayed total at rank << 1, remaining total beside it, so one line carries both.
  std::vector<std::int64_t> totals_;

  std::vector<std::uint64_t> bits0_;
  std::vector<std::uint64_t> bits1_;
  std::vector<std::uint64_t> bits2_;

  std::int32_t best_ = EMPTY;
};

}  // namespace exchange::matcher
