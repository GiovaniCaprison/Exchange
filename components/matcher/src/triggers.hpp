// The stops that have not fired, chained through the slab's own links in arrival order. A resting
// stop is not book liquidity: it is a condition evaluated against the last executed price, and on
// firing it becomes an ordinary order. The chain is scanned on firing, deliberately: firing order
// is arrival order among the reached, which the chain already is because stops only ever join at
// the back, and a stop book stays small enough that an index would cost more than the walk it
// saves (P-5).

#pragma once

#include <cstdint>
#include <vector>

#include "slab.hpp"

namespace exchange::matcher {

class Triggers {
 public:
  explicit Triggers(Slab& slab) : slab_(slab) {}

  void add(const std::int32_t slot) {
    Slab::Hot& entry = slab_.hot(slot);
    entry.previous = static_cast<std::uint32_t>(tail_);
    entry.next = 0;
    if (tail_ == 0) {
      head_ = slot;
    } else {
      slab_.hot(tail_).next = static_cast<std::uint32_t>(slot);
    }
    tail_ = slot;
  }

  void remove(const std::int32_t slot) {
    Slab::Hot& entry = slab_.hot(slot);
    const std::int32_t previous = static_cast<std::int32_t>(entry.previous);
    const std::int32_t next = static_cast<std::int32_t>(entry.next);
    if (previous == 0) {
      head_ = next;
    } else {
      slab_.hot(previous).next = static_cast<std::uint32_t>(next);
    }
    if (next == 0) {
      tail_ = previous;
    } else {
      slab_.hot(next).previous = static_cast<std::uint32_t>(previous);
    }
    entry.previous = 0;
    entry.next = 0;
  }

  std::int32_t named(const std::uint32_t participantId, const std::uint64_t clientOrderId) const {
    for (std::int32_t slot = head_; slot != 0;
         slot = static_cast<std::int32_t>(slab_.hot(slot).next)) {
      if (slab_.cold(slot).participantId == participantId &&
          slab_.cold(slot).clientOrderId == clientOrderId) {
        return slot;
      }
    }
    return 0;
  }

  void of(const std::uint32_t participantId, std::vector<std::int32_t>& into) const {
    for (std::int32_t slot = head_; slot != 0;
         slot = static_cast<std::int32_t>(slab_.hot(slot).next)) {
      if (slab_.cold(slot).participantId == participantId) {
        into.push_back(slot);
      }
    }
  }

  // Moves the stops the last executed price has reached into the caller's queue, earliest first,
  // removed as they go.
  void fire(const std::int64_t lastExecutedPrice, std::vector<std::int32_t>& into) {
    std::int32_t slot = head_;
    while (slot != 0) {
      const std::int32_t following = static_cast<std::int32_t>(slab_.hot(slot).next);
      const bool reached = slab_.cold(slot).side == 0
                               ? lastExecutedPrice >= slab_.cold(slot).triggerPrice
                               : lastExecutedPrice <= slab_.cold(slot).triggerPrice;
      if (reached) {
        remove(slot);
        into.push_back(slot);
      }
      slot = following;
    }
  }

  std::int32_t headSlot() const { return head_; }

  void save(ByteSink& sink) const {
    sink.i32(head_);
    sink.i32(tail_);
  }

  void restore(ByteSource& source) {
    head_ = source.i32();
    tail_ = source.i32();
  }

 private:
  Slab& slab_;
  std::int32_t head_ = 0;
  std::int32_t tail_ = 0;
};

}  // namespace exchange::matcher
