// The participant's order entry client (docs/components/ecosystem.md): the session protocol
// spoken from the other side. Socket free like the gateway it talks to, so the suites can drive
// both machines byte by byte: whatever owns the transport shuttles received() and outbound()
// between them. A connection opens with a login naming the session sequence expected next, the
// gateway replays the participant's stream from there byte exactly, and because the client
// counts every owned event, a reconnection resumes with nothing delivered twice and nothing
// lost, which is the session contract exercised from the seat that pays for it.
//
// The client tracks its own consequences: live orders in a fixed table keyed by clientOrderId,
// the engine's orderId learned from acceptances, positions signed by its own intents. A fresh
// client that logs in from sequence zero rebuilds its resting book from the replayed stream;
// positions it can only sign for orders whose side the replay itself reveals, because a real
// participant persists its intents, and this library does not pretend otherwise (OUCH's replay
// exists to repair message loss, not amnesia).

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/CommandRefused.h"
#include "exchange_protocol/LoginAccepted.h"
#include "exchange_protocol/LoginRejected.h"
#include "exchange_protocol/LoginRequest.h"
#include "exchange_protocol/LogoutRequest.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRejected.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/OrderTriggered.h"
#include "exchange_protocol/ReplaceOrder.h"
#include "exchange_protocol/SessionEnded.h"
#include "exchange_protocol/SessionHeartbeat.h"
#include "wire.hpp"

namespace exchange::ecosystem {

namespace sbe = ::exchange::protocol;

template <typename Clock>
class OrderEntry {
 public:
  static constexpr std::size_t ORDERS = 1 << 14;
  static constexpr std::size_t POSITIONS = 64;

  OrderEntry(Clock& clock, const std::uint32_t participantId, const std::uint64_t credential,
             const std::uint64_t heartbeatNanos = 1'000'000'000ULL)
      : clock_(clock),
        participantId_(participantId),
        credential_(credential),
        heartbeat_(heartbeatNanos) {
    out_.reserve(OUT_BYTES);
    orderKeys_.assign(ORDERS, 0);
    orders_.resize(ORDERS);
    idKeys_.assign(ORDERS, 0);
    idOwners_.assign(ORDERS, 0);
    instruments_.reserve(POSITIONS);
    positions_.reserve(POSITIONS);
    latencies_.assign(LATENCIES, 0);
  }

  // The transport surface, driven by whatever owns the socket ---------------------------------

  // A fresh connection: log in expecting the sequence after the last owned event seen, zero on
  // the client's first ever connection, so the gateway's replay starts exactly where this client
  // stopped hearing.
  void connected() {
    reassembler_.reset();
    established_ = false;
    lastInbound_ = clock_.now();
    lastOutbound_ = 0;
    encode<sbe::LoginRequest>([this](sbe::LoginRequest& login) {
      login.expectedSequence(seen_ == 0 ? 0 : seen_ + 1)
          .credential(credential_)
          .participantId(participantId_)
          .reserved(0);
    });
  }

  void disconnected() {
    established_ = false;
    out_.clear();
    drained_ = 0;
  }

  void received(const char* bytes, const std::size_t length) {
    lastInbound_ = clock_.now();
    const bool sound = reassembler_.feed(
        bytes, length, [this](char* message, const std::size_t size) { onMessage(message, size); });
    if (!sound) {
      // A garbled stream from the venue's side is a dead transport, nothing subtler.
      established_ = false;
    }
  }

  std::pair<const char*, std::size_t> outbound() const {
    return {out_.data() + drained_, out_.size() - drained_};
  }

  void drainedBy(const std::size_t bytes) {
    drained_ += bytes;
    if (drained_ == out_.size()) {
      out_.clear();
      drained_ = 0;
    }
  }

  // The pulse both ways: speak on the interval, and notice the venue going silent.
  void onTick() {
    const std::uint64_t now = clock_.now();
    if (established_ && now - lastOutbound_ >= heartbeat_) {
      encode<sbe::SessionHeartbeat>([](sbe::SessionHeartbeat& pulse) { pulse.reserved(0); });
    }
  }

  bool starving() const { return established_ && clock_.now() - lastInbound_ > DEAD * heartbeat_; }
  bool established() const { return established_; }

  // The orders ---------------------------------------------------------------------------------

  std::uint64_t newOrder(const std::uint32_t instrumentId, const sbe::Side::Value side,
                         const sbe::Pricing::Value pricing,
                         const sbe::TimeInForce::Value timeInForce, const std::int64_t price,
                         const std::int64_t quantity, const std::int64_t minQuantity = 0,
                         const std::int64_t displayQuantity = 0,
                         const std::int64_t triggerPrice = 0, const std::uint64_t smpId = 0,
                         const bool auctionOnly = false) {
    const std::uint64_t clientOrderId = nextClientOrderId_++;
    Order& order = orderSlot(clientOrderId);
    order.clientOrderId = clientOrderId;
    order.orderId = 0;
    order.instrumentId = instrumentId;
    order.price = price;
    order.remaining = quantity;
    order.side = static_cast<std::uint8_t>(side == sbe::Side::BUY ? 0 : 1);
    order.submittedAt = clock_.now();
    order.live = false;
    command<sbe::NewOrder>(instrumentId, [&](sbe::NewOrder& out) {
      out.clientOrderId(clientOrderId)
          .price(price)
          .quantity(quantity)
          .minQuantity(minQuantity)
          .displayQuantity(displayQuantity)
          .triggerPrice(triggerPrice)
          .smpId(smpId)
          .participantId(participantId_);
      out.side(side).pricing(pricing).timeInForce(timeInForce);
      out.flags().clear().auctionOnly(auctionOnly);
    });
    return clientOrderId;
  }

  void cancel(const std::uint64_t clientOrderId, const std::uint32_t instrumentId) {
    command<sbe::CancelOrder>(instrumentId, [&](sbe::CancelOrder& out) {
      out.clientOrderId(clientOrderId).participantId(participantId_);
    });
  }

  void replace(const std::uint64_t clientOrderId, const std::uint32_t instrumentId,
               const std::int64_t quantity, const std::int64_t price) {
    command<sbe::ReplaceOrder>(instrumentId, [&](sbe::ReplaceOrder& out) {
      out.clientOrderId(clientOrderId)
          .quantity(quantity)
          .price(price)
          .participantId(participantId_);
    });
  }

  void logout() {
    encode<sbe::LogoutRequest>([](sbe::LogoutRequest& out) { out.reserved(0); });
  }

  // The client's view of its own consequences --------------------------------------------------

  std::int64_t positionOf(const std::uint32_t instrumentId) const {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return positions_[at];
      }
    }
    return 0;
  }

  bool liveOf(const std::uint64_t clientOrderId) const {
    const long at = orderFind(clientOrderId);
    return at >= 0 && orders_[static_cast<std::size_t>(at)].live;
  }

  std::int64_t remainingOf(const std::uint64_t clientOrderId) const {
    const long at = orderFind(clientOrderId);
    return at < 0 ? 0 : orders_[static_cast<std::size_t>(at)].remaining;
  }

  template <typename Handler>
  void forEachLive(Handler&& handler) const {
    for (std::size_t at = 0; at < ORDERS; at++) {
      if (orderKeys_[at] != 0 && orders_[at].live) {
        handler(orders_[at]);
      }
    }
  }

  // One measured acceptance round trip in nanoseconds, zero when none is waiting. The owner
  // drains these into whatever series discipline it keeps (P-14).
  std::uint64_t drainAcceptanceLatency() {
    if (latencyOut_ == latencyIn_) {
      return 0;
    }
    return latencies_[latencyOut_++ & (LATENCIES - 1)];
  }

  std::uint64_t seen() const { return seen_; }
  std::uint64_t executions() const { return executions_; }
  std::uint64_t rejections() const { return rejections_; }
  std::uint64_t refusals() const { return refusals_; }
  std::uint64_t liveOrders() const {
    std::uint64_t count = 0;
    forEachLive([&](const auto&) { count++; });
    return count;
  }

  struct Order {
    std::uint64_t clientOrderId = 0;
    std::uint64_t orderId = 0;
    std::uint32_t instrumentId = 0;
    std::int64_t price = 0;
    std::int64_t remaining = 0;
    std::uint64_t submittedAt = 0;
    std::uint8_t side = 0;  // 0 buy, 1 sell, 2 unknown to a replaying amnesiac
    bool live = false;
  };

 private:
  static constexpr std::size_t OUT_BYTES = 1 << 20;
  static constexpr std::uint64_t LATENCIES = 1 << 12;
  static constexpr std::uint64_t DEAD = 5;
  static constexpr std::uint8_t UNKNOWN_SIDE = 2;

  // The wire out: one framed message, encoded in place at the tail of the buffer.
  template <typename Message, typename Fill>
  void encode(Fill&& fill) {
    const std::size_t length = sbe::MessageHeader::encodedLength() + Message::sbeBlockLength();
    const std::size_t start = out_.size();
    out_.resize(start + sizeof(std::uint16_t) + length);
    const std::uint16_t prefix = static_cast<std::uint16_t>(length);
    std::memcpy(out_.data() + start, &prefix, sizeof prefix);
    Message message;
    message.wrapAndApplyHeader(out_.data() + start + sizeof prefix, 0, length);
    fill(message);
    lastOutbound_ = clock_.now();
  }

  // A command carries zeroed stamps, the instrument, and this session's identity; the sequencer
  // writes the stamps and the gateway rewrites the identity whatever a client put there.
  template <typename Message, typename Fill>
  void command(const std::uint32_t instrumentId, Fill&& fill) {
    encode<Message>([&](Message& out) {
      out.context().sequence(0).timestamp(0).instrumentId(instrumentId).reserved(0);
      fill(out);
    });
  }

  void onMessage(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::LoginAccepted::sbeTemplateId(): {
        sbe::LoginAccepted accepted;
        accepted.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                               length);
        established_ = true;
        // Replay begins where the gateway says; everything before it is already counted.
        seen_ = accepted.nextSequence() - 1;
        break;
      }
      case sbe::LoginRejected::sbeTemplateId():
      case sbe::SessionEnded::sbeTemplateId():
        established_ = false;
        break;
      case sbe::SessionHeartbeat::sbeTemplateId():
        break;
      case sbe::CommandRefused::sbeTemplateId(): {
        // The gate said no on the session plane: the command never reached the venue, so the
        // intent dies here and the stream will never mention it.
        sbe::CommandRefused refused;
        refused.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        const long at = orderFind(refused.clientOrderId());
        if (at >= 0 && !orders_[static_cast<std::size_t>(at)].live) {
          orderErase(static_cast<std::size_t>(at));
        }
        refusals_++;
        break;
      }
      default:
        onOwnedEvent(message, length, wrap);
        break;
    }
  }

  void onOwnedEvent(char* message, const std::size_t length, sbe::MessageHeader& wrap) {
    switch (wrap.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        sbe::OrderAccepted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        Order& order = orderSlot(event.clientOrderId());
        if (order.clientOrderId == 0) {
          // A replayed acceptance this client no longer has the intent for: hold the identifiers
          // and let the rest of the replay fill in what it can.
          order.clientOrderId = event.clientOrderId();
          order.instrumentId = event.context().instrumentId();
          order.side = UNKNOWN_SIDE;
          order.remaining = 0;
        }
        order.orderId = event.orderId();
        order.live = true;
        idSlot(event.orderId()) = event.clientOrderId();
        // The wire-to-wire fact, measured from the seat that pays for it: submission written to
        // acceptance heard, on this client's clock. Replayed acceptances carry no intent stamp
        // and record nothing.
        if (order.submittedAt != 0) {
          if (latencyIn_ - latencyOut_ == LATENCIES) {
            latencyOut_++;
          }
          const std::uint64_t took = clock_.now() - order.submittedAt;
          latencies_[latencyIn_++ & (LATENCIES - 1)] = took == 0 ? 1 : took;
          order.submittedAt = 0;
        }
        break;
      }
      case sbe::OrderRejected::sbeTemplateId(): {
        sbe::OrderRejected event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        // A rejection for an order already accepted is a refused cancel or a rolled-back
        // replace: the resting order stands and so does the client's entry. Only a pending new
        // order dies of its rejection.
        const long at = orderFind(event.clientOrderId());
        if (at >= 0 && !orders_[static_cast<std::size_t>(at)].live) {
          orderErase(static_cast<std::size_t>(at));
        }
        rejections_++;
        break;
      }
      case sbe::OrderRested::sbeTemplateId(): {
        sbe::OrderRested event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = orderFind(idOf(event.orderId()));
        if (at >= 0) {
          Order& order = orders_[static_cast<std::size_t>(at)];
          order.price = event.price();
          // A rest is the book speaking: the order is live whatever came before it, which is
          // how a replacement, removed and re-rested under the same ids, comes back.
          order.live = true;
          if (order.side == UNKNOWN_SIDE) {
            order.side = static_cast<std::uint8_t>(event.side() == sbe::Side::BUY ? 0 : 1);
            order.remaining = event.quantity();
          }
        }
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        sbe::OrderExecuted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        // An execution reduces whichever of its two orders this client holds, both when the
        // client crossed with itself in an auction.
        applyExecution(idOf(event.aggressorOrderId()), event.quantity());
        if (event.restingOrderId() != event.aggressorOrderId()) {
          applyExecution(idOf(event.restingOrderId()), event.quantity());
        }
        executions_++;
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = orderFind(idOf(event.orderId()));
        if (at >= 0) {
          orders_[static_cast<std::size_t>(at)].remaining = event.quantity();
        }
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        sbe::OrderRemoved event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const std::uint64_t clientOrderId = idOf(event.orderId());
        const long at = orderFind(clientOrderId);
        if (at >= 0) {
          if (event.reason() == sbe::RemoveReason::REPLACED) {
            // The replacement continues under the same clientOrderId: the intent survives the
            // removal and the acceptance that follows brings the new engine orderId.
            orders_[static_cast<std::size_t>(at)].live = false;
          } else {
            orderErase(static_cast<std::size_t>(at));
          }
        }
        break;
      }
      case sbe::OrderTriggered::sbeTemplateId():
        break;
      default:
        return;
    }
    seen_++;
  }

  void applyExecution(const std::uint64_t clientOrderId, const std::int64_t quantity) {
    const long at = orderFind(clientOrderId);
    if (at < 0) {
      return;
    }
    Order& order = orders_[static_cast<std::size_t>(at)];
    order.remaining -= quantity;
    if (order.side != UNKNOWN_SIDE) {
      book(order.instrumentId, order.side == 0 ? quantity : -quantity);
    }
    if (order.remaining <= 0) {
      orderErase(static_cast<std::size_t>(at));
    }
  }

  void book(const std::uint32_t instrumentId, const std::int64_t signedQuantity) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        positions_[at] += signedQuantity;
        return;
      }
    }
    instruments_.push_back(instrumentId);
    positions_.push_back(signedQuantity);
  }

  // Two fixed tables, the discipline the gateway keeps: orders by clientOrderId, and the
  // engine's orderId mapped back, both linear probing with backward-shift deletion.
  static std::size_t hashOf(const std::uint64_t key) {
    std::uint64_t mixed = key * 0x9E3779B97F4A7C15ULL;
    mixed ^= mixed >> 32;
    return static_cast<std::size_t>(mixed) & (ORDERS - 1);
  }

  Order& orderSlot(const std::uint64_t key) {
    std::size_t at = hashOf(key);
    while (orderKeys_[at] != 0 && orderKeys_[at] != key) {
      at = (at + 1) & (ORDERS - 1);
    }
    if (orderKeys_[at] == 0) {
      orders_[at] = Order{};
    }
    orderKeys_[at] = key;
    return orders_[at];
  }

  long orderFind(const std::uint64_t key) const {
    if (key == 0) {
      return -1;
    }
    std::size_t at = hashOf(key);
    while (orderKeys_[at] != 0) {
      if (orderKeys_[at] == key) {
        return static_cast<long>(at);
      }
      at = (at + 1) & (ORDERS - 1);
    }
    return -1;
  }

  void orderErase(std::size_t at) {
    // The dead order leaves both tables, or the fixed orderId index fills with ghosts.
    if (orders_[at].orderId != 0) {
      idErase(orders_[at].orderId);
    }
    orderKeys_[at] = 0;
    std::size_t hole = at;
    std::size_t probe = (at + 1) & (ORDERS - 1);
    while (orderKeys_[probe] != 0) {
      const std::size_t wants = hashOf(orderKeys_[probe]);
      const bool movable = ((probe - wants) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1));
      if (movable) {
        orderKeys_[hole] = orderKeys_[probe];
        orders_[hole] = orders_[probe];
        orderKeys_[probe] = 0;
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
  }

  std::uint64_t& idSlot(const std::uint64_t key) {
    std::size_t at = hashOf(key);
    while (idKeys_[at] != 0 && idKeys_[at] != key) {
      at = (at + 1) & (ORDERS - 1);
    }
    idKeys_[at] = key;
    return idOwners_[at];
  }

  std::uint64_t idOf(const std::uint64_t key) const {
    std::size_t at = hashOf(key);
    while (idKeys_[at] != 0) {
      if (idKeys_[at] == key) {
        return idOwners_[at];
      }
      at = (at + 1) & (ORDERS - 1);
    }
    return 0;
  }

  void idErase(const std::uint64_t key) {
    std::size_t at = hashOf(key);
    while (idKeys_[at] != 0 && idKeys_[at] != key) {
      at = (at + 1) & (ORDERS - 1);
    }
    if (idKeys_[at] == 0) {
      return;
    }
    idKeys_[at] = 0;
    std::size_t hole = at;
    std::size_t probe = (at + 1) & (ORDERS - 1);
    while (idKeys_[probe] != 0) {
      const std::size_t wants = hashOf(idKeys_[probe]);
      const bool movable = ((probe - wants) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1));
      if (movable) {
        idKeys_[hole] = idKeys_[probe];
        idOwners_[hole] = idOwners_[probe];
        idKeys_[probe] = 0;
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
  }

  Clock& clock_;
  std::uint32_t participantId_;
  std::uint64_t credential_;
  std::uint64_t heartbeat_;
  gateway::Reassembler reassembler_;
  std::vector<char> out_;
  std::size_t drained_ = 0;
  bool established_ = false;
  std::uint64_t seen_ = 0;
  std::uint64_t nextClientOrderId_ = 1;
  std::uint64_t lastInbound_ = 0;
  std::uint64_t lastOutbound_ = 0;
  std::uint64_t executions_ = 0;
  std::uint64_t rejections_ = 0;
  std::uint64_t refusals_ = 0;
  std::vector<std::uint64_t> orderKeys_;
  std::vector<Order> orders_;
  std::vector<std::uint64_t> idKeys_;
  std::vector<std::uint64_t> idOwners_;
  std::vector<std::uint32_t> instruments_;
  std::vector<std::int64_t> positions_;
  std::vector<std::uint64_t> latencies_;
  std::uint64_t latencyIn_ = 0;
  std::uint64_t latencyOut_ = 0;
};

}  // namespace exchange::ecosystem
