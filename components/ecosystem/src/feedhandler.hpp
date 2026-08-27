// The participant's feed handler (docs/components/ecosystem.md): public packets in, a queryable
// book out. This is the consumer library's packet source doing its real job: A and B twins
// arrive through the same door, whichever lands first is the one that counts, a gap parks the
// packet that revealed it and asks the rewinder, and nothing is ever delivered out of order. On
// top of that sits the visible book, maintained by the same public rules PROTOCOL states for any
// outsider, with the touch kept incrementally because a strategy asks for it on every decision.
//
// Orders live in one fixed open-addressing table, arrival order preserved, backward-shift
// deletion, zero steady-state allocation; when the best level empties the instrument's touch is
// rebuilt by one scan of the table, which is the price of fixed storage and lands on the cold
// side of a level clearing.

#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/PublicAuctionIndicative.h"
#include "exchange_protocol/PublicOrderAdded.h"
#include "exchange_protocol/PublicOrderExecuted.h"
#include "exchange_protocol/PublicOrderReduced.h"
#include "exchange_protocol/PublicOrderRemoved.h"
#include "exchange_protocol/PublicSessionState.h"
#include "exchange_protocol/PublicSessionSummary.h"
#include "exchange_protocol/SnapshotComplete.h"
#include "ranges.hpp"
#include "stream.hpp"

namespace exchange::ecosystem {

namespace sbe = ::exchange::protocol;

struct Touch {
  std::int64_t bidPrice = 0;
  std::int64_t bidQuantity = 0;
  std::int64_t askPrice = 0;
  std::int64_t askQuantity = 0;
};

struct LastTrade {
  std::int64_t price = 0;
  std::int64_t quantity = 0;
  std::uint64_t count = 0;
};

struct SessionSummary {
  std::int64_t open = 0;
  std::int64_t high = 0;
  std::int64_t low = 0;
  std::int64_t close = 0;
  std::int64_t volume = 0;
  std::uint64_t prints = 0;
};

template <typename Rewind>
class FeedHandler {
 public:
  static constexpr std::size_t ORDERS = 1 << 17;

  // A handler that joins at the feed's start consumes live packets from sequence one; a late
  // joiner asks for the snapshot first, feeds it through onSnapshotRange, and the snapshot's
  // last word names where the live feed continues. Live packets fed before that word are
  // dropped, which is the Glimpse joiner's contract: snapshot first, then the feed.
  explicit FeedHandler(Rewind& rewind, const bool viaSnapshot = false) : rewind_(rewind) {
    keys_.assign(ORDERS, 0);
    orders_.resize(ORDERS);
    instruments_.reserve(64);
    states_.reserve(64);
    touches_.reserve(64);
    trades_.reserve(64);
    summaries_.reserve(64);
    indicatives_.reserve(64);
    if (!viaSnapshot) {
      source_.emplace(1, 1, rewind_);
    }
  }

  // Either twin's packet, through the same door.
  void onPacket(char* bytes, const std::size_t length) {
    if (!source_.has_value()) {
      return;
    }
    source_->onPacket(bytes, length,
                      [this](char* message, const std::size_t size) { onPublic(message, size); });
  }

  // One snapshot range from a glimpse connection, applied in the order served.
  void onSnapshotRange(char* bytes, const std::size_t length) {
    common::ranges::Reader reader(bytes, length);
    reader.forEach([this](char* message, const std::size_t size) {
      sbe::MessageHeader wrap;
      wrap.wrap(message, 0, 0, size);
      if (wrap.templateId() == sbe::SnapshotComplete::sbeTemplateId()) {
        sbe::SnapshotComplete complete;
        complete.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                               size);
        source_.emplace(complete.nextSequence(), 1, rewind_);
        return;
      }
      onPublic(message, size);
    });
  }

  bool joined() const { return source_.has_value(); }
  std::uint64_t nextSequence() const { return source_.has_value() ? source_->nextSequence() : 0; }
  bool ended() const { return source_.has_value() && source_->ended(); }

  // The strategy's questions -------------------------------------------------------------------

  Touch touchOf(const std::uint32_t instrumentId) const {
    const long at = indexOf(instrumentId);
    return at < 0 ? Touch{} : touches_[static_cast<std::size_t>(at)];
  }

  LastTrade lastTradeOf(const std::uint32_t instrumentId) const {
    const long at = indexOf(instrumentId);
    return at < 0 ? LastTrade{} : trades_[static_cast<std::size_t>(at)];
  }

  sbe::SessionState::Value stateOf(const std::uint32_t instrumentId) const {
    const long at = indexOf(instrumentId);
    return at < 0 ? sbe::SessionState::CLOSED : states_[static_cast<std::size_t>(at)];
  }

  SessionSummary summaryOf(const std::uint32_t instrumentId) const {
    const long at = indexOf(instrumentId);
    return at < 0 ? SessionSummary{} : summaries_[static_cast<std::size_t>(at)];
  }

  std::int64_t indicativeOf(const std::uint32_t instrumentId) const {
    const long at = indexOf(instrumentId);
    return at < 0 ? 0 : indicatives_[static_cast<std::size_t>(at)];
  }

  const std::vector<std::uint32_t>& instruments() const { return instruments_; }

  // Every displayed order, for the proofs that hold this book to the reference.
  template <typename Handler>
  void forEachOrder(Handler&& handler) const {
    for (std::size_t at = 0; at < ORDERS; at++) {
      if (keys_[at] != 0) {
        handler(orders_[at]);
      }
    }
  }

 private:
  struct Order {
    std::uint64_t id = 0;
    std::uint64_t arrival = 0;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    std::uint32_t instrumentId = 0;
    std::uint8_t side = 0;  // 0 buy, 1 sell
  };

 public:
  using PublicOrder = Order;

 private:
  void onPublic(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::PublicOrderAdded::sbeTemplateId(): {
        sbe::PublicOrderAdded added;
        added.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const std::size_t instrument = noteInstrument(added.context().instrumentId());
        // An add for an id already displayed is the venue requeueing it (an iceberg refill); the
        // old display leaves the touch and the new one joins, unless the leaving emptied the
        // level and the rescan already saw the new state.
        const long displayed = find(added.orderId());
        const std::int64_t oldPrice =
            displayed < 0 ? 0 : orders_[static_cast<std::size_t>(displayed)].price;
        const std::int64_t oldQuantity =
            displayed < 0 ? 0 : orders_[static_cast<std::size_t>(displayed)].quantity;
        Order& order = slotFor(added.orderId());
        order.id = added.orderId();
        order.arrival = ++arrivals_;
        order.price = added.price();
        order.quantity = added.quantity();
        order.instrumentId = added.context().instrumentId();
        order.side = static_cast<std::uint8_t>(added.side() == sbe::Side::BUY ? 0 : 1);
        const bool rescanned =
            displayed >= 0 && applyDelta(instrument, order.side, oldPrice, -oldQuantity);
        if (!rescanned) {
          applyDelta(instrument, order.side, order.price, order.quantity);
        }
        break;
      }
      case sbe::PublicOrderExecuted::sbeTemplateId(): {
        sbe::PublicOrderExecuted executed;
        executed.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                               length);
        const std::size_t instrument = noteInstrument(executed.context().instrumentId());
        LastTrade& trade = trades_[instrument];
        trade.price = executed.price();
        trade.quantity = executed.quantity();
        trade.count++;
        const long at = find(executed.orderId());
        if (at >= 0) {
          Order& order = orders_[static_cast<std::size_t>(at)];
          order.quantity -= executed.quantity();
          const std::uint8_t side = order.side;
          const std::int64_t price = order.price;
          if (order.quantity <= 0) {
            erase(static_cast<std::size_t>(at));
          }
          applyDelta(instrument, side, price, -executed.quantity());
        }
        break;
      }
      case sbe::PublicOrderReduced::sbeTemplateId(): {
        sbe::PublicOrderReduced reduced;
        reduced.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        const long at = find(reduced.orderId());
        if (at >= 0) {
          Order& order = orders_[static_cast<std::size_t>(at)];
          const std::size_t instrument = noteInstrument(order.instrumentId);
          const std::int64_t delta = reduced.quantity() - order.quantity;
          order.quantity = reduced.quantity();
          applyDelta(instrument, order.side, order.price, delta);
        }
        break;
      }
      case sbe::PublicOrderRemoved::sbeTemplateId(): {
        sbe::PublicOrderRemoved removed;
        removed.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        const long at = find(removed.orderId());
        if (at >= 0) {
          const Order order = orders_[static_cast<std::size_t>(at)];
          const std::size_t instrument = noteInstrument(order.instrumentId);
          erase(static_cast<std::size_t>(at));
          applyDelta(instrument, order.side, order.price, -order.quantity);
        }
        break;
      }
      case sbe::PublicSessionState::sbeTemplateId(): {
        sbe::PublicSessionState state;
        state.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const std::size_t instrument = noteInstrument(state.context().instrumentId());
        states_[instrument] = state.state();
        break;
      }
      case sbe::PublicSessionSummary::sbeTemplateId(): {
        sbe::PublicSessionSummary summary;
        summary.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        const std::size_t instrument = noteInstrument(summary.context().instrumentId());
        summaries_[instrument] = {summary.openPrice(),  summary.highPrice(), summary.lowPrice(),
                                  summary.closePrice(), summary.volume(),    summary.prints()};
        break;
      }
      case sbe::PublicAuctionIndicative::sbeTemplateId(): {
        sbe::PublicAuctionIndicative indicative;
        indicative.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                                 length);
        const std::size_t instrument = noteInstrument(indicative.context().instrumentId());
        indicatives_[instrument] = indicative.price();
        break;
      }
      default:
        // The conflated touch is for consumers without a book; this handler keeps its own.
        break;
    }
  }

  // Touch maintenance: a signed delta of displayed quantity at one price, applied strictly after
  // the order table reached its final state, because an emptied level rebuilds the whole side
  // from that table. Returns true when it rescanned, so a caller applying two deltas for one
  // event knows the second is already reflected.
  bool applyDelta(const std::size_t instrument, const std::uint8_t side, const std::int64_t price,
                  const std::int64_t delta) {
    if (delta == 0) {
      return false;
    }
    Touch& touch = touches_[instrument];
    std::int64_t& bestPrice = side == 0 ? touch.bidPrice : touch.askPrice;
    std::int64_t& bestDepth = side == 0 ? touch.bidQuantity : touch.askQuantity;
    if (delta > 0) {
      if (bestDepth == 0 || (side == 0 ? price > bestPrice : price < bestPrice)) {
        // Strictly better than the standing touch means nothing else displays at this price.
        bestPrice = price;
        bestDepth = delta;
      } else if (price == bestPrice) {
        bestDepth += delta;
      }
      return false;
    }
    if (bestDepth > 0 && price == bestPrice) {
      bestDepth += delta;
      if (bestDepth <= 0) {
        rescan(instrument, side);
        return true;
      }
    }
    return false;
  }

  void rescan(const std::size_t instrument, const std::uint8_t side) {
    const std::uint32_t instrumentId = instruments_[instrument];
    std::int64_t best = 0;
    std::int64_t depth = 0;
    for (std::size_t at = 0; at < ORDERS; at++) {
      if (keys_[at] == 0) {
        continue;
      }
      const Order& order = orders_[at];
      if (order.instrumentId != instrumentId || order.side != side || order.quantity <= 0) {
        continue;
      }
      const bool better = depth == 0 || (side == 0 ? order.price > best : order.price < best);
      if (better) {
        best = order.price;
        depth = order.quantity;
      } else if (order.price == best) {
        depth += order.quantity;
      }
    }
    Touch& touch = touches_[instrument];
    if (side == 0) {
      touch.bidPrice = best;
      touch.bidQuantity = depth;
    } else {
      touch.askPrice = best;
      touch.askQuantity = depth;
    }
  }

  // The fixed table: linear probing on the order id, backward-shift deletion so tombstones never
  // accumulate, the discipline the venue's own tables keep.
  static std::size_t hashOf(const std::uint64_t key) {
    std::uint64_t mixed = key * 0x9E3779B97F4A7C15ULL;
    mixed ^= mixed >> 32;
    return static_cast<std::size_t>(mixed) & (ORDERS - 1);
  }

  Order& slotFor(const std::uint64_t key) {
    std::size_t at = hashOf(key);
    while (keys_[at] != 0 && keys_[at] != key) {
      at = (at + 1) & (ORDERS - 1);
    }
    keys_[at] = key;
    return orders_[at];
  }

  long find(const std::uint64_t key) const {
    std::size_t at = hashOf(key);
    while (keys_[at] != 0) {
      if (keys_[at] == key) {
        return static_cast<long>(at);
      }
      at = (at + 1) & (ORDERS - 1);
    }
    return -1;
  }

  void erase(std::size_t at) {
    keys_[at] = 0;
    std::size_t hole = at;
    std::size_t probe = (at + 1) & (ORDERS - 1);
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

  std::size_t noteInstrument(const std::uint32_t instrumentId) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return at;
      }
    }
    instruments_.push_back(instrumentId);
    states_.push_back(sbe::SessionState::CLOSED);
    touches_.push_back({});
    trades_.push_back({});
    summaries_.push_back({});
    indicatives_.push_back(0);
    return instruments_.size() - 1;
  }

  long indexOf(const std::uint32_t instrumentId) const {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return static_cast<long>(at);
      }
    }
    return -1;
  }

  Rewind& rewind_;
  std::optional<common::stream::PacketSource<Rewind>> source_;
  std::vector<std::uint64_t> keys_;
  std::vector<Order> orders_;
  std::uint64_t arrivals_ = 0;
  std::vector<std::uint32_t> instruments_;
  std::vector<sbe::SessionState::Value> states_;
  std::vector<Touch> touches_;
  std::vector<LastTrade> trades_;
  std::vector<SessionSummary> summaries_;
  std::vector<std::int64_t> indicatives_;
};

}  // namespace exchange::ecosystem
