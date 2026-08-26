// The matcher's output, encoded straight into the space the ring hands over (P-12). The feed is a
// template on the ring type, so claim and commit inline to plain stores and no event pays a
// dispatched call. Every event carries the partition's own event sequence, the input sequence of
// the command it answers, and that command's timestamp carried unchanged (P-3), which is what
// keeps a replayed stream byte identical to the live one (P-2).
//
// The ring contract, all of it a precondition (P-9): claim(length) returns an eight byte aligned
// offset into buffer() with length bytes usable; commit() publishes the last claim; publish() is
// called once per command after its last event, which is where a ring makes the whole command's
// events visible with one release. Nothing is buffered here and nothing is held back: every event
// leaves the visible book valid on its own.

#pragma once

#include <cstdint>

#include "bytes.hpp"
#include "exchange_protocol/AuctionIndicative.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRejected.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/OrderTriggered.h"
#include "exchange_protocol/RejectReason.h"
#include "exchange_protocol/RemoveReason.h"
#include "exchange_protocol/SessionState.h"
#include "exchange_protocol/SessionStateChanged.h"
#include "exchange_protocol/Side.h"

namespace exchange::matcher {

namespace sbe = ::exchange::protocol;

template <typename Ring>
class Feed {
 public:
  explicit Feed(Ring& ring) : ring_(ring) {}

  // The command being answered: stored once per command, stamped onto every event it causes.
  void answering(const std::uint64_t inputSequence, const std::uint64_t timestamp,
                 const std::uint32_t instrumentId) {
    inputSequence_ = inputSequence;
    timestamp_ = timestamp;
    instrumentId_ = instrumentId;
  }

  void accepted(const std::uint64_t orderId, const std::uint64_t clientOrderId,
                const std::uint32_t participantId) {
    auto encoder = claimed<sbe::OrderAccepted>();
    encoder.orderId(orderId).clientOrderId(clientOrderId).participantId(participantId);
    ring_.commit();
  }

  void rejected(const std::uint64_t clientOrderId, const std::uint32_t participantId,
                const sbe::RejectReason::Value reason) {
    auto encoder = claimed<sbe::OrderRejected>();
    encoder.clientOrderId(clientOrderId).participantId(participantId).reason(reason);
    ring_.commit();
  }

  // Displayed quantity only. Hidden quantity is never reported.
  void rested(const std::uint64_t orderId, const std::int64_t price, const std::int64_t displayed,
              const std::int32_t side) {
    auto encoder = claimed<sbe::OrderRested>();
    encoder.orderId(orderId).price(price).quantity(displayed).side(
        static_cast<sbe::Side::Value>(side));
    ring_.commit();
  }

  void executed(const std::uint64_t executionId, const std::uint64_t aggressor,
                const std::uint64_t resting, const std::int64_t price,
                const std::int64_t quantity) {
    auto encoder = claimed<sbe::OrderExecuted>();
    encoder.executionId(executionId)
        .aggressorOrderId(aggressor)
        .restingOrderId(resting)
        .price(price)
        .quantity(quantity);
    ring_.commit();
  }

  void reduced(const std::uint64_t orderId, const std::int64_t displayed) {
    auto encoder = claimed<sbe::OrderReduced>();
    encoder.orderId(orderId).quantity(displayed);
    ring_.commit();
  }

  void removed(const std::uint64_t orderId, const std::int64_t quantity,
               const sbe::RemoveReason::Value reason) {
    auto encoder = claimed<sbe::OrderRemoved>();
    encoder.orderId(orderId).quantity(quantity).reason(reason);
    ring_.commit();
  }

  void triggered(const std::uint64_t orderId) {
    auto encoder = claimed<sbe::OrderTriggered>();
    encoder.orderId(orderId);
    ring_.commit();
  }

  void stateChanged(const std::int32_t state) {
    auto encoder = claimed<sbe::SessionStateChanged>();
    encoder.state(static_cast<sbe::SessionState::Value>(state));
    ring_.commit();
  }

  void indicative(const std::int64_t price, const std::int64_t quantity) {
    auto encoder = claimed<sbe::AuctionIndicative>();
    encoder.price(price).quantity(quantity);
    ring_.commit();
  }

  // The command is answered in full; the ring makes its events visible as one batch.
  void finish() { ring_.publish(); }

  // The feed's only durable state is its stream position; the per-command context is transient.
  void save(common::ByteSink& sink) const { sink.u64(sequence_); }
  void restore(common::ByteSource& source) { sequence_ = source.u64(); }

 private:
  // Claims space for one event, wraps its header, and stamps the event context: the partition's
  // own sequence, the attribution to the input, and the carried timestamp.
  template <typename Encoder>
  Encoder claimed() {
    const std::size_t length = sbe::MessageHeader::encodedLength() + Encoder::sbeBlockLength();
    const std::size_t at = ring_.claim(length);
    Encoder encoder;
    encoder.wrapAndApplyHeader(ring_.buffer(), at, at + length);
    encoder.context()
        .sequence(++sequence_)
        .inputSequence(inputSequence_)
        .timestamp(timestamp_)
        .instrumentId(instrumentId_)
        .reserved(0);
    return encoder;
  }

  Ring& ring_;
  std::uint64_t sequence_ = 0;
  std::uint64_t inputSequence_ = 0;
  std::uint64_t timestamp_ = 0;
  std::uint32_t instrumentId_ = 0;
};

}  // namespace exchange::matcher
