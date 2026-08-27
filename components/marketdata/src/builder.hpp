// The book builder: the matcher's events in, the public vocabulary out, and the visible book
// maintained by the same rules PROTOCOL states for any consumer, which makes this component the
// standing proof that the event stream rebuilds the state it describes (P-1). Attribution never
// crosses: the public messages have no field to carry it, so the strip is structural. Orders
// live in one fixed open-addressing table with queue priority kept as an arrival number, so a
// snapshot can hand a late joiner the book in the order fairness happened.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "exchange_protocol/AuctionIndicative.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/PublicAuctionIndicative.h"
#include "exchange_protocol/PublicOrderAdded.h"
#include "exchange_protocol/PublicOrderExecuted.h"
#include "exchange_protocol/PublicOrderReduced.h"
#include "exchange_protocol/PublicOrderRemoved.h"
#include "exchange_protocol/PublicSessionState.h"
#include "exchange_protocol/PublicSessionSummary.h"
#include "exchange_protocol/PublicTopOfBook.h"
#include "exchange_protocol/SessionStateChanged.h"

namespace exchange::marketdata {

namespace sbe = ::exchange::protocol;

struct PublicOrder {
  std::uint64_t id = 0;
  std::uint64_t arrival = 0;
  std::int64_t price = 0;
  std::int64_t quantity = 0;
  std::uint32_t instrumentId = 0;
  std::uint8_t side = 0;
};

template <typename Publisher>
class Builder {
 public:
  // One session's official numbers for one instrument, reset when a new session pre-opens.
  struct Tape {
    std::int64_t open = 0;
    std::int64_t high = 0;
    std::int64_t low = 0;
    std::int64_t close = 0;
    std::int64_t volume = 0;
    std::uint64_t prints = 0;
  };

  static constexpr std::size_t ORDERS = 1 << 17;

  explicit Builder(Publisher& publisher) : publisher_(publisher) {
    keys_.assign(ORDERS, 0);
    orders_.resize(ORDERS);
    instruments_.reserve(64);
    states_.reserve(64);
    tapes_.reserve(64);
  }

  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::OrderRested::sbeTemplateId(): {
        sbe::OrderRested event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        noteInstrument(event.context().instrumentId());
        lastTimestamp_ = event.context().timestamp();
        // A rest for an id already displayed is the venue requeueing it; the arrival renews
        // either way, because queue priority did.
        PublicOrder& order = slotFor(event.orderId());
        order.id = event.orderId();
        order.arrival = ++arrivals_;
        order.price = event.price();
        order.quantity = event.quantity();
        order.instrumentId = event.context().instrumentId();
        order.side = static_cast<std::uint8_t>(event.side() == sbe::Side::BUY ? 0 : 1);
        publish<sbe::PublicOrderAdded>(event.context().instrumentId(), [&](auto& out) {
          out.orderId(event.orderId()).price(event.price()).quantity(event.quantity());
          out.side(event.side());
        });
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        sbe::OrderExecuted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        lastTimestamp_ = event.context().timestamp();
        // The consumer rule made public: the execution reduces whichever of its orders the book
        // holds, and the world hears about each book order it touched.
        executed(event.context().instrumentId(), event.aggressorOrderId(), event.executionId(),
                 event.price(), event.quantity());
        executed(event.context().instrumentId(), event.restingOrderId(), event.executionId(),
                 event.price(), event.quantity());
        // The session's tape counts the print once, whichever book orders it touched.
        {
          Tape& day = tape(event.context().instrumentId());
          if (day.prints == 0) {
            day.open = event.price();
            day.high = event.price();
            day.low = event.price();
          }
          day.high = event.price() > day.high ? event.price() : day.high;
          day.low = event.price() < day.low ? event.price() : day.low;
          day.close = event.price();
          day.volume += event.quantity();
          day.prints++;
        }
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        lastTimestamp_ = event.context().timestamp();
        PublicOrder* order = find(event.orderId());
        if (order != nullptr) {
          order->quantity = event.quantity();
          publish<sbe::PublicOrderReduced>(event.context().instrumentId(), [&](auto& out) {
            out.orderId(event.orderId()).quantity(event.quantity());
          });
        }
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        sbe::OrderRemoved event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        lastTimestamp_ = event.context().timestamp();
        if (erase(event.orderId())) {
          publish<sbe::PublicOrderRemoved>(event.context().instrumentId(),
                                           [&](auto& out) { out.orderId(event.orderId()); });
        }
        break;
      }
      case sbe::SessionStateChanged::sbeTemplateId(): {
        sbe::SessionStateChanged event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        lastTimestamp_ = event.context().timestamp();
        noteInstrument(event.context().instrumentId());
        stateOfMutable(event.context().instrumentId()) = event.state();
        publish<sbe::PublicSessionState>(event.context().instrumentId(),
                                         [&](auto& out) { out.state(event.state()); });
        if (event.state() == sbe::SessionState::CLOSED) {
          // The session's last word: the official numbers of the day, the close being the last
          // print, which is the closing cross when one ran.
          const Tape& day = tape(event.context().instrumentId());
          if (day.prints > 0) {
            publish<sbe::PublicSessionSummary>(event.context().instrumentId(), [&](auto& out) {
              out.openPrice(day.open)
                  .highPrice(day.high)
                  .lowPrice(day.low)
                  .closePrice(day.close)
                  .volume(day.volume)
                  .prints(day.prints);
            });
          }
        } else if (event.state() == sbe::SessionState::PRE_OPEN) {
          tape(event.context().instrumentId()) = Tape{};
        }
        break;
      }
      case sbe::AuctionIndicative::sbeTemplateId(): {
        sbe::AuctionIndicative event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        lastTimestamp_ = event.context().timestamp();
        publish<sbe::PublicAuctionIndicative>(event.context().instrumentId(), [&](auto& out) {
          out.price(event.price()).quantity(event.quantity());
        });
        break;
      }
      default:
        // Acceptances, refusals and triggers are private or book neutral; the feed says nothing.
        break;
    }
  }

  // The conflated touch: one scan per instrument, deterministic because sums are order free and
  // instruments emit in discovery order. A real feed keeps aggregated levels; at a conflation
  // interval the scan is cheaper than the machinery and the interval hides it.
  void onConflation() {
    for (const std::uint32_t instrument : instruments_) {
      std::int64_t bidPrice = 0;
      std::int64_t bidQuantity = 0;
      std::int64_t askPrice = 0;
      std::int64_t askQuantity = 0;
      for (std::size_t at = 0; at < ORDERS; at++) {
        if (keys_[at] == 0 || orders_[at].instrumentId != instrument) {
          continue;
        }
        const PublicOrder& order = orders_[at];
        if (order.side == 0) {
          if (order.price > bidPrice) {
            bidPrice = order.price;
            bidQuantity = order.quantity;
          } else if (order.price == bidPrice) {
            bidQuantity += order.quantity;
          }
        } else {
          if (askPrice == 0 || order.price < askPrice) {
            askPrice = order.price;
            askQuantity = order.quantity;
          } else if (order.price == askPrice) {
            askQuantity += order.quantity;
          }
        }
      }
      publish<sbe::PublicTopOfBook>(instrument, [&](auto& out) {
        out.bidPrice(bidPrice).bidQuantity(bidQuantity).askPrice(askPrice).askQuantity(askQuantity);
      });
    }
  }

  // The snapshot's reading surface.
  const std::vector<std::uint32_t>& instruments() const { return instruments_; }
  sbe::SessionState::Value stateOf(const std::uint32_t instrument) const {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrument) {
        return states_[at];
      }
    }
    return sbe::SessionState::CLOSED;
  }
  template <typename Handler>
  void forEachOrder(Handler&& handler) const {
    for (std::size_t at = 0; at < ORDERS; at++) {
      if (keys_[at] != 0) {
        handler(orders_[at]);
      }
    }
  }
  std::uint64_t lastTimestamp() const { return lastTimestamp_; }

 private:
  void executed(const std::uint32_t instrument, const std::uint64_t orderId,
                const std::uint64_t executionId, const std::int64_t price,
                const std::int64_t quantity) {
    PublicOrder* order = find(orderId);
    if (order == nullptr) {
      return;
    }
    publish<sbe::PublicOrderExecuted>(instrument, [&](auto& out) {
      out.orderId(orderId).executionId(executionId).price(price).quantity(quantity);
    });
    order->quantity -= quantity;
    if (order->quantity <= 0) {
      erase(orderId);
    }
  }

  template <typename Message, typename Fill>
  void publish(const std::uint32_t instrument, Fill&& fill) {
    const std::size_t size = sbe::MessageHeader::encodedLength() + Message::sbeBlockLength();
    char* space = publisher_.claim(size);
    Message out;
    out.wrapAndApplyHeader(space, 0, size);
    out.context().timestamp(lastTimestamp_).instrumentId(instrument).reserved(0);
    fill(out);
    publisher_.commit(size);
  }

  void noteInstrument(const std::uint32_t instrument) {
    for (const std::uint32_t known : instruments_) {
      if (known == instrument) {
        return;
      }
    }
    instruments_.push_back(instrument);
    states_.push_back(sbe::SessionState::CLOSED);
    tapes_.emplace_back();
  }

 public:
  Tape tapeOf(const std::uint32_t instrument) const {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrument) {
        return tapes_[at];
      }
    }
    return {};
  }

 private:
  Tape& tape(const std::uint32_t instrument) {
    noteInstrument(instrument);
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrument) {
        return tapes_[at];
      }
    }
    throw std::logic_error("the tape of an instrument never noted");
  }

  sbe::SessionState::Value& stateOfMutable(const std::uint32_t instrument) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrument) {
        return states_[at];
      }
    }
    throw std::logic_error("state of an instrument never noted");
  }

  PublicOrder& slotFor(const std::uint64_t orderId) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (keys_[slot] != 0 && keys_[slot] != orderId) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    if (keys_[slot] == 0) {
      if (++count_ > (ORDERS * 3) / 4) {
        throw std::runtime_error("the public book overflowed; size it for the session");
      }
      keys_[slot] = orderId;
    }
    return orders_[slot];
  }

  PublicOrder* find(const std::uint64_t orderId) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (keys_[slot] != 0) {
      if (keys_[slot] == orderId) {
        return &orders_[slot];
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
    return nullptr;
  }

  // Backward-shift deletion keeps probes honest without tombstones, the same move the matcher's
  // name index makes.
  bool erase(const std::uint64_t orderId) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (keys_[slot] != 0 && keys_[slot] != orderId) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    if (keys_[slot] == 0) {
      return false;
    }
    count_--;
    std::size_t hole = slot;
    std::size_t probe = (hole + 1) & (ORDERS - 1);
    while (keys_[probe] != 0) {
      const std::size_t home = keys_[probe] & (ORDERS - 1);
      const bool movable = ((probe - home) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1));
      if (movable) {
        keys_[hole] = keys_[probe];
        orders_[hole] = orders_[probe];
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
    keys_[hole] = 0;
    return true;
  }

  Publisher& publisher_;
  std::vector<std::uint64_t> keys_;
  std::vector<PublicOrder> orders_;
  std::vector<std::uint32_t> instruments_;
  std::vector<sbe::SessionState::Value> states_;
  std::vector<Tape> tapes_;
  std::size_t count_ = 0;
  std::uint64_t arrivals_ = 0;
  std::uint64_t lastTimestamp_ = 0;
};

}  // namespace exchange::marketdata
