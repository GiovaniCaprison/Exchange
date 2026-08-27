// The gate's risk engine (docs/PROTOCOL.md, the gate's risk checks): every client command passes
// here before it can cost a place in the global order, the checks run cheapest first, and the
// refusal is the first failing check's typed reason. Credit is a ledger accounted from the
// admission side, full notional reserved at the admitted price and drained by the venue's own
// events, deliberately ahead of the truth between admission and answer; conservation, the ledger
// draining to zero when every order closes, is what the invariants hold. Cancellations pass
// everything but the throttle, because a venue never blocks the message that reduces risk.
//
// The venue's events reconcile the ledger with three tells: a removal whose reason is REPLACED
// belongs to a replacement the admission already re-priced; a rejection for an entry that never
// received an order id was the new order itself; and a rejection while a replacement is pending
// rolls the entry back to what it was. Everything else either drains or releases.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/MassCancel.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRejected.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/RiskRefusal.h"

namespace exchange::risk {

namespace sbe = ::exchange::protocol;

struct Limits {
  std::int64_t maxQuantity = 1'000'000;
  std::int64_t maxNotional = 1'000'000'000;
  std::int64_t credit = 10'000'000'000;
  // Absolute width around the last execution the gate has seen; zero disables the collar.
  std::int64_t collarWidth = 0;
  std::uint32_t ratePerSecond = 1'000;
  std::uint32_t burst = 100;
};

// What the gateway decoded from the client's command; the gate never touches the wire itself.
struct Intent {
  std::uint16_t templateId = 0;
  std::uint64_t clientOrderId = 0;
  std::uint32_t instrumentId = 0;
  std::int64_t price = 0;
  std::int64_t quantity = 0;
  bool buying = false;
  bool market = false;
};

struct Verdict {
  bool admitted = true;
  sbe::RiskRefusal::Value reason = sbe::RiskRefusal::RATE_LIMIT;
};

template <typename Clock>
class Risk {
 public:
  static constexpr std::size_t ORDERS = 1 << 17;
  static constexpr std::size_t INSTRUMENTS = 64;

  Risk(Clock& clock, std::vector<std::pair<std::uint32_t, Limits>> table)
      : clock_(clock), table_(std::move(table)) {
    accounts_.resize(table_.size());
    const std::uint64_t now = clock_.now();
    for (std::size_t at = 0; at < table_.size(); at++) {
      accounts_[at].tokens = static_cast<std::uint64_t>(table_[at].second.burst) * SCALE;
      accounts_[at].refilled = now;
    }
    keys_.assign(ORDERS, 0);
    entries_.resize(ORDERS);
    orderKeys_.assign(ORDERS, 0);
    orderSlots_.assign(ORDERS, 0);
    references_.reserve(INSTRUMENTS);
  }

  Verdict admit(const std::uint32_t participant, const Intent& intent) {
    const std::size_t account = accountOf(participant);
    Account& mine = accounts_[account];
    const Limits& limits = table_[account].second;

    // The throttle first and for everything: a token bucket on the injected clock.
    const std::uint64_t now = clock_.now();
    const std::uint64_t ceiling = static_cast<std::uint64_t>(limits.burst) * SCALE;
    mine.tokens += (now - mine.refilled) * limits.ratePerSecond;
    mine.refilled = now;
    if (mine.tokens > ceiling) {
      mine.tokens = ceiling;
    }
    if (mine.tokens < SCALE) {
      refused_++;
      return {false, sbe::RiskRefusal::RATE_LIMIT};
    }
    mine.tokens -= SCALE;

    // Risk-reducing messages pass everything else.
    if (intent.templateId == sbe::CancelOrder::sbeTemplateId() ||
        intent.templateId == sbe::MassCancel::sbeTemplateId()) {
      admitted_++;
      return {true, sbe::RiskRefusal::RATE_LIMIT};
    }

    if (intent.quantity > limits.maxQuantity) {
      refused_++;
      return {false, sbe::RiskRefusal::MAX_ORDER_SIZE};
    }
    const std::int64_t reference = referenceOf(intent.instrumentId);
    const std::int64_t effectivePrice = intent.market ? reference : intent.price;
    if (intent.market && reference == 0) {
      // An unbounded notional cannot be reserved, so it cannot be admitted.
      refused_++;
      return {false, sbe::RiskRefusal::CREDIT};
    }
    const std::int64_t notional = effectivePrice * intent.quantity;
    if (notional > limits.maxNotional) {
      refused_++;
      return {false, sbe::RiskRefusal::MAX_NOTIONAL};
    }

    if (intent.templateId == sbe::NewOrder::sbeTemplateId()) {
      if (find(participant, intent.clientOrderId) != nullptr) {
        refused_++;
        return {false, sbe::RiskRefusal::DUPLICATE};
      }
      if (!intent.market && limits.collarWidth > 0 && reference > 0) {
        const std::int64_t away =
            intent.price > reference ? intent.price - reference : reference - intent.price;
        if (away > limits.collarWidth) {
          refused_++;
          return {false, sbe::RiskRefusal::PRICE_COLLAR};
        }
      }
      if (mine.ledger + magnitude(mine.position) + notional > limits.credit) {
        refused_++;
        return {false, sbe::RiskRefusal::CREDIT};
      }
      Entry& entry = insert(participant, intent.clientOrderId);
      entry.price = effectivePrice;
      entry.remaining = intent.quantity;
      entry.executed = 0;
      entry.orderId = 0;
      entry.account = static_cast<std::uint32_t>(account);
      entry.buying = intent.buying;
      entry.backupValid = false;
      mine.ledger += notional;
      admitted_++;
      return {true, sbe::RiskRefusal::RATE_LIMIT};
    }

    // A replace: checked on its new values, the reservation re-priced now, and the old shape
    // remembered until the venue answers, so a rejected replace rolls back.
    Entry* entry = find(participant, intent.clientOrderId);
    if (!intent.market && limits.collarWidth > 0 && reference > 0) {
      const std::int64_t away =
          intent.price > reference ? intent.price - reference : reference - intent.price;
      if (away > limits.collarWidth) {
        refused_++;
        return {false, sbe::RiskRefusal::PRICE_COLLAR};
      }
    }
    if (entry == nullptr) {
      // Replacing what the gate never admitted: nothing to reserve, and the venue will answer.
      admitted_++;
      return {true, sbe::RiskRefusal::RATE_LIMIT};
    }
    const std::int64_t newRemaining =
        intent.quantity > entry->executed ? intent.quantity - entry->executed : 0;
    const std::int64_t before = entry->price * entry->remaining;
    const std::int64_t after = effectivePrice * newRemaining;
    if (after > before &&
        mine.ledger + magnitude(mine.position) + (after - before) > limits.credit) {
      refused_++;
      return {false, sbe::RiskRefusal::CREDIT};
    }
    entry->backupPrice = entry->price;
    entry->backupRemaining = entry->remaining;
    entry->backupValid = true;
    entry->price = effectivePrice;
    entry->remaining = newRemaining;
    mine.ledger += after - before;
    admitted_++;
    return {true, sbe::RiskRefusal::RATE_LIMIT};
  }

  // The venue's events, reconciling the ledger and teaching the collar its reference.
  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        sbe::OrderAccepted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        Entry* entry = find(event.participantId(), event.clientOrderId());
        if (entry != nullptr) {
          entry->orderId = event.orderId();
          entry->backupValid = false;
          link(event.orderId(), entry);
        }
        break;
      }
      case sbe::OrderRejected::sbeTemplateId(): {
        sbe::OrderRejected event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        Entry* entry = find(event.participantId(), event.clientOrderId());
        if (entry == nullptr) {
          break;
        }
        if (entry->orderId == 0) {
          // The new order itself was refused: release everything it reserved.
          release(*entry);
        } else if (entry->backupValid) {
          // A replacement was refused: the order stands as it stood.
          Account& mine = accounts_[entry->account];
          mine.ledger +=
              entry->backupPrice * entry->backupRemaining - entry->price * entry->remaining;
          entry->price = entry->backupPrice;
          entry->remaining = entry->backupRemaining;
          entry->backupValid = false;
        }
        // A refused cancellation changes nothing.
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        sbe::OrderExecuted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        noteReference(instrumentOf(message), event.price());
        drain(event.aggressorOrderId(), event.price(), event.quantity());
        drain(event.restingOrderId(), event.price(), event.quantity());
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        // The queue-keeping face of a successful replace: the admission's rollback is void.
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        Entry* entry = findByOrder(event.orderId());
        if (entry != nullptr) {
          entry->backupValid = false;
        }
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        sbe::OrderRemoved event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        if (event.reason() == sbe::RemoveReason::REPLACED) {
          // The replacement continues under the same order id; admission already re-priced it.
          Entry* entry = findByOrder(event.orderId());
          if (entry != nullptr) {
            entry->backupValid = false;
          }
          break;
        }
        Entry* entry = findByOrder(event.orderId());
        if (entry != nullptr) {
          release(*entry);
        }
        break;
      }
      default:
        break;
    }
  }

  std::int64_t exposure(const std::uint32_t participant) const {
    return accounts_[accountOf(participant)].ledger;
  }
  std::int64_t position(const std::uint32_t participant) const {
    return accounts_[accountOf(participant)].position;
  }
  std::uint64_t admitted() const { return admitted_; }
  std::uint64_t refused() const { return refused_; }

 private:
  static constexpr std::uint64_t SCALE = 1'000'000'000ULL;

  struct Account {
    std::int64_t ledger = 0;
    std::int64_t position = 0;
    std::uint64_t tokens = 0;
    std::uint64_t refilled = 0;
  };

  struct Entry {
    std::uint64_t clientOrderId = 0;
    std::uint64_t orderId = 0;
    std::int64_t price = 0;
    std::int64_t remaining = 0;
    std::int64_t executed = 0;
    std::int64_t backupPrice = 0;
    std::int64_t backupRemaining = 0;
    std::uint32_t participant = 0;
    std::uint32_t account = 0;
    bool buying = false;
    bool backupValid = false;
  };

  static std::int64_t magnitude(const std::int64_t value) { return value < 0 ? -value : value; }

  static std::uint32_t instrumentOf(const char* message) {
    // Every event's context carries the instrument after sequence, input sequence and timestamp.
    std::uint32_t instrument = 0;
    std::memcpy(&instrument,
                message + sbe::MessageHeader::encodedLength() + 3 * sizeof(std::uint64_t),
                sizeof instrument);
    return instrument;
  }

  std::size_t accountOf(const std::uint32_t participant) const {
    for (std::size_t at = 0; at < table_.size(); at++) {
      if (table_[at].first == participant) {
        return at;
      }
    }
    throw std::logic_error("risk asked about a participant it was never given");
  }

  std::int64_t referenceOf(const std::uint32_t instrument) const {
    for (const auto& [known, price] : references_) {
      if (known == instrument) {
        return price;
      }
    }
    return 0;
  }

  void noteReference(const std::uint32_t instrument, const std::int64_t price) {
    for (auto& [known, reference] : references_) {
      if (known == instrument) {
        reference = price;
        return;
      }
    }
    references_.emplace_back(instrument, price);
  }

  void drain(const std::uint64_t orderId, const std::int64_t price, const std::int64_t quantity) {
    Entry* entry = findByOrder(orderId);
    if (entry == nullptr) {
      return;
    }
    const std::int64_t fill = quantity < entry->remaining ? quantity : entry->remaining;
    Account& mine = accounts_[entry->account];
    mine.ledger -= entry->price * fill;
    mine.position += entry->buying ? price * quantity : -(price * quantity);
    entry->remaining -= fill;
    entry->executed += fill;
    if (entry->remaining <= 0) {
      erase(*entry);
    }
  }

  void release(Entry& entry) {
    accounts_[entry.account].ledger -= entry.price * entry.remaining;
    erase(entry);
  }

  // The (participant, clientOrderId) table and the orderId index, both fixed open addressing
  // with backward-shift deletion, the house pattern.
  static std::uint64_t keyOf(const std::uint32_t participant, const std::uint64_t clientOrderId) {
    return (static_cast<std::uint64_t>(participant) * 0x9E3779B97F4A7C15ULL) ^ clientOrderId;
  }

  Entry& insert(const std::uint32_t participant, const std::uint64_t clientOrderId) {
    if (++count_ > (ORDERS * 3) / 4) {
      throw std::runtime_error("the risk ledger overflowed; size it for the session");
    }
    const std::uint64_t key = keyOf(participant, clientOrderId) | 1ULL;
    std::size_t slot = key & (ORDERS - 1);
    while (keys_[slot] != 0) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    keys_[slot] = key;
    Entry& entry = entries_[slot];
    entry.clientOrderId = clientOrderId;
    entry.participant = participant;
    return entry;
  }

  Entry* find(const std::uint32_t participant, const std::uint64_t clientOrderId) {
    const std::uint64_t key = keyOf(participant, clientOrderId) | 1ULL;
    std::size_t slot = key & (ORDERS - 1);
    while (keys_[slot] != 0) {
      if (keys_[slot] == key && entries_[slot].participant == participant &&
          entries_[slot].clientOrderId == clientOrderId) {
        return &entries_[slot];
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
    return nullptr;
  }

  void link(const std::uint64_t orderId, Entry* entry) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (orderKeys_[slot] != 0) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    orderKeys_[slot] = orderId;
    orderSlots_[slot] = static_cast<std::uint32_t>(entry - entries_.data());
  }

  Entry* findByOrder(const std::uint64_t orderId) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (orderKeys_[slot] != 0) {
      if (orderKeys_[slot] == orderId) {
        return &entries_[orderSlots_[slot]];
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
    return nullptr;
  }

  void erase(Entry& entry) {
    count_--;
    if (entry.orderId != 0) {
      eraseIn(orderKeys_, orderSlots_, entry.orderId & (ORDERS - 1), entry.orderId);
    }
    const std::uint64_t key = keyOf(entry.participant, entry.clientOrderId) | 1ULL;
    std::size_t slot = key & (ORDERS - 1);
    while (keys_[slot] != 0 && &entries_[slot] != &entry) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    // Backward-shift deletion over paired arrays keeps probes honest without tombstones.
    std::size_t hole = slot;
    std::size_t probe = (hole + 1) & (ORDERS - 1);
    while (keys_[probe] != 0) {
      const std::size_t home = keys_[probe] & (ORDERS - 1);
      if (((probe - home) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1))) {
        keys_[hole] = keys_[probe];
        entries_[hole] = entries_[probe];
        if (entries_[hole].orderId != 0) {
          relink(entries_[hole].orderId, hole);
        }
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
    keys_[hole] = 0;
  }

  void eraseIn(std::vector<std::uint64_t>& keys, std::vector<std::uint32_t>& values,
               std::size_t slot, const std::uint64_t key) {
    while (keys[slot] != 0 && keys[slot] != key) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    if (keys[slot] == 0) {
      return;
    }
    std::size_t hole = slot;
    std::size_t probe = (hole + 1) & (ORDERS - 1);
    while (keys[probe] != 0) {
      const std::size_t home = keys[probe] & (ORDERS - 1);
      if (((probe - home) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1))) {
        keys[hole] = keys[probe];
        values[hole] = values[probe];
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
    keys[hole] = 0;
  }

  void relink(const std::uint64_t orderId, const std::size_t entrySlot) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (orderKeys_[slot] != 0) {
      if (orderKeys_[slot] == orderId) {
        orderSlots_[slot] = static_cast<std::uint32_t>(entrySlot);
        return;
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
  }

  Clock& clock_;
  std::vector<std::pair<std::uint32_t, Limits>> table_;
  std::vector<Account> accounts_;
  std::vector<std::uint64_t> keys_;
  std::vector<Entry> entries_;
  std::vector<std::uint64_t> orderKeys_;
  std::vector<std::uint32_t> orderSlots_;
  std::vector<std::pair<std::uint32_t, std::int64_t>> references_;
  std::size_t count_ = 0;
  std::uint64_t admitted_ = 0;
  std::uint64_t refused_ = 0;
};

}  // namespace exchange::risk
