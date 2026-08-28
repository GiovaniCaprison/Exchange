// Every resting order in one book, as slots in two preallocated arrays: a hot array the match loop
// touches per fill, one cache line per order, and a cold array holding identity and the entry-time
// qualifiers, touched once per command at most. An order is an int slot; a field access is an
// array read at a computed offset; the free list threads through the same links the queues use, so
// a slot is always in exactly one chain (P-8). Slot zero is reserved as the null link, which makes
// a freshly zeroed array already empty.
//
// Nothing here is validated (P-9) and nothing is cleared on release beyond the link word, so a
// released slot still answers reads with its final values until it is reissued, which the auction
// uncrossing relies on. A slot's state is a function of its most recent write (P-8). Growth
// doubles the arrays and is paid on the way up to the high-water mark of live orders; the steady
// state after it allocates nothing (P-6), and every slot index stays valid across growth because
// an index is an index rather than an address.

#pragma once

#include <cstdint>
#include <vector>

#include "bytes.hpp"

namespace exchange::matcher {

class Slab {
 public:
  // What one execution touches, padded to exactly one cache line.
  struct alignas(64) Hot {
    std::int64_t remaining;
    std::int64_t displayed;
    std::int64_t executed;
    std::uint64_t id;
    std::int64_t arrival;
    std::uint64_t smpId;
    std::uint32_t previous;
    std::uint32_t next;
    std::int32_t tick;
    std::uint32_t pad;
  };
  static_assert(sizeof(Hot) == 64);

  // Identity and the entry-time qualifiers, read when an order is named or re-shaped.
  struct Cold {
    std::uint64_t clientOrderId;
    std::int64_t minQuantity;
    std::int64_t triggerPrice;
    std::int64_t displaySize;
    std::uint32_t participantId;
    std::uint8_t side;
    std::uint8_t pricing;
    std::uint8_t timeInForce;
    std::uint8_t postOnly;
    std::uint8_t auctionOnly;
  };

  explicit Slab(const std::int32_t preallocated) : capacity_(preallocated) {
    hot_.resize(static_cast<std::size_t>(capacity_));
    cold_.resize(static_cast<std::size_t>(capacity_));
    thread(1);
  }

  Hot& hot(const std::int32_t slot) { return hot_[static_cast<std::size_t>(slot)]; }
  const Hot& hot(const std::int32_t slot) const { return hot_[static_cast<std::size_t>(slot)]; }
  Cold& cold(const std::int32_t slot) { return cold_[static_cast<std::size_t>(slot)]; }
  const Cold& cold(const std::int32_t slot) const { return cold_[static_cast<std::size_t>(slot)]; }

  std::int32_t acquire() {
    if (freeHead_ == 0) {
      const std::int32_t first = capacity_;
      capacity_ *= 2;
      hot_.resize(static_cast<std::size_t>(capacity_));
      cold_.resize(static_cast<std::size_t>(capacity_));
      thread(first);
    }
    const std::int32_t slot = freeHead_;
    freeHead_ = static_cast<std::int32_t>(hot(slot).next);
    hot(slot).previous = 0;
    hot(slot).next = 0;
    return slot;
  }

  // The slot must already be out of every chain (P-8); only the link words are rewritten.
  void release(const std::int32_t slot) {
    hot(slot).previous = 0;
    hot(slot).next = static_cast<std::uint32_t>(freeHead_);
    freeHead_ = slot;
  }

  // A snapshot is a copy of the arrays, free list threading and all, so a restored slab is
  // bit-equal to the one that was saved and the suffix it produces is byte identical (P-2).
  void save(common::ByteSink& sink) const {
    sink.i32(capacity_);
    sink.i32(freeHead_);
    sink.span(hot_);
    sink.span(cold_);
  }

  void restore(common::ByteSource& source) {
    capacity_ = source.i32();
    freeHead_ = source.i32();
    source.span(hot_);
    source.span(cold_);
  }

 private:
  void thread(const std::int32_t first) {
    for (std::int32_t slot = capacity_ - 1; slot >= first; slot--) {
      hot_[static_cast<std::size_t>(slot)].next = static_cast<std::uint32_t>(freeHead_);
      freeHead_ = slot;
    }
  }

  std::vector<Hot> hot_;
  std::vector<Cold> cold_;
  std::int32_t capacity_;
  std::int32_t freeHead_ = 0;
};

}  // namespace exchange::matcher
