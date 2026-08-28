// The ownership table every routing consumer keeps: who an engine orderId belongs to, learned
// from acceptances, read on every event that names an order. The discipline is the lesson this
// repository learned three times before writing it down once: a fixed open-addressing table
// whose entries never leave saturates however big it is, and a saturated open table probes
// forever. So entries leave when their orders die, a replacement survives its removal because
// the same ids continue, and the fully-filled aggressors no removal ever names age out by
// orderId distance on a periodic sweep, deterministically, because orderIds are the engine's own
// monotone numbering.
//
// The table stores the facts every consumer of it has wanted so far: owner, instrument, side,
// price, remaining and whether it rested. Readers use find() and at() before applying the
// hygiene verbs, because a consumer usually needs to route or account an event before the entry
// it names retires.

#pragma once

#include <cstdint>
#include <vector>

namespace exchange::common {

struct OwnedOrder {
  std::uint64_t orderId = 0;
  std::uint32_t participant = 0;
  std::uint32_t instrument = 0;
  std::int64_t price = 0;
  std::int64_t remaining = 0;
  std::uint8_t side = 2;  // 0 buy, 1 sell, 2 not yet rested
  bool rested = false;
};

class Ownership {
 public:
  static constexpr std::size_t ORDERS = 1 << 17;
  static constexpr std::uint64_t AGE_HORIZON = 1 << 16;
  static constexpr std::size_t SWEEP_EVERY = 4096;

  Ownership() {
    keys_.assign(ORDERS, 0);
    orders_.resize(ORDERS);
  }

  OwnedOrder& accepted(const std::uint64_t orderId, const std::uint32_t participant,
                       const std::uint32_t instrument) {
    std::size_t slot = hashOf(orderId);
    while (keys_[slot] != 0 && keys_[slot] != orderId) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    keys_[slot] = orderId;
    orders_[slot] = {orderId, participant, instrument, 0, 0, 2, false};
    if (orderId > newestOrderId_) {
      newestOrderId_ = orderId;
    }
    if (++accepted_ % SWEEP_EVERY == 0) {
      sweep();
    }
    return orders_[slot];
  }

  long find(const std::uint64_t orderId) const {
    std::size_t slot = hashOf(orderId);
    while (keys_[slot] != 0) {
      if (keys_[slot] == orderId) {
        return static_cast<long>(slot);
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
    return -1;
  }

  OwnedOrder& at(const long slot) { return orders_[static_cast<std::size_t>(slot)]; }
  const OwnedOrder& at(const long slot) const { return orders_[static_cast<std::size_t>(slot)]; }

  // The hygiene verbs, applied after the caller has read what it needed ------------------------

  void onRested(const std::uint64_t orderId, const std::uint8_t side, const std::int64_t price,
                const std::int64_t quantity) {
    const long slot = find(orderId);
    if (slot >= 0) {
      OwnedOrder& order = at(slot);
      order.rested = true;
      order.side = side;
      order.price = price;
      order.remaining = quantity;
    }
  }

  void onReduced(const std::uint64_t orderId, const std::int64_t quantity) {
    const long slot = find(orderId);
    if (slot >= 0) {
      at(slot).remaining = quantity;
    }
  }

  // A rested order that filled whole is done; no removal will ever name it.
  void onExecuted(const std::uint64_t restingOrderId, const std::int64_t quantity) {
    const long slot = find(restingOrderId);
    if (slot >= 0) {
      OwnedOrder& order = at(slot);
      order.remaining -= quantity;
      if (order.rested && order.remaining <= 0) {
        erase(static_cast<std::size_t>(slot));
      }
    }
  }

  // A replacement continues under the same ids, so its removal keeps the entry.
  void onRemoved(const std::uint64_t orderId, const bool replaced) {
    const long slot = find(orderId);
    if (slot >= 0 && !replaced) {
      erase(static_cast<std::size_t>(slot));
    }
  }

 private:
  static std::size_t hashOf(const std::uint64_t key) {
    std::uint64_t mixed = key * 0x9E3779B97F4A7C15ULL;
    mixed ^= mixed >> 32;
    return static_cast<std::size_t>(mixed) & (ORDERS - 1);
  }

  void erase(std::size_t slot) {
    keys_[slot] = 0;
    std::size_t hole = slot;
    std::size_t probe = (slot + 1) & (ORDERS - 1);
    while (keys_[probe] != 0) {
      const std::size_t wants = hashOf(keys_[probe]);
      const bool movable = ((probe - wants) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1));
      if (movable) {
        keys_[hole] = keys_[probe];
        orders_[hole] = orders_[probe];
        keys_[probe] = 0;
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
  }

  void sweep() {
    if (newestOrderId_ <= AGE_HORIZON) {
      return;
    }
    const std::uint64_t oldest = newestOrderId_ - AGE_HORIZON;
    for (std::size_t slot = 0; slot < ORDERS; slot++) {
      if (keys_[slot] != 0 && !orders_[slot].rested && orders_[slot].orderId < oldest) {
        erase(slot);
        slot--;
      }
    }
  }

  std::vector<std::uint64_t> keys_;
  std::vector<OwnedOrder> orders_;
  std::uint64_t newestOrderId_ = 0;
  std::uint64_t accepted_ = 0;
};

}  // namespace exchange::common
