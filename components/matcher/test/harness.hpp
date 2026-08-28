// The test-side machinery every matcher suite shares: a ring that captures what was committed, a
// writer that plays the sequencer (assigning sequence numbers and stamping the one timestamp,
// deterministically), and a reader that decodes captured events back into a comparable form.

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "exchange_protocol/AuctionIndicative.h"
#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/InstrumentDefinition.h"
#include "exchange_protocol/MassCancel.h"
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
#include "exchange_protocol/SessionControl.h"
#include "exchange_protocol/SessionStateChanged.h"

namespace exchange::matcher::test {

namespace sbe = ::exchange::protocol;

// A ring in miniature: one buffer, a cursor that wraps at eight byte alignment, the last claim
// held so a commit knows what to capture, and a publish counter so a test can see the per-command
// batching happen.
class CapturingRing {
 public:
  std::size_t claim(const std::size_t length) {
    const std::size_t aligned = (length + 7) & ~std::size_t{7};
    if (cursor_ + aligned > space_.size()) {
      cursor_ = 0;
    }
    claimed_ = cursor_;
    claimedLength_ = length;
    cursor_ += aligned;
    return claimed_;
  }

  char* buffer() { return space_.data(); }

  void commit() {
    captured_.insert(captured_.end(), space_.begin() + static_cast<long>(claimed_),
                     space_.begin() + static_cast<long>(claimed_ + claimedLength_));
  }

  void publish() { publishes_++; }

  const std::vector<char>& captured() const { return captured_; }
  std::uint64_t publishes() const { return publishes_; }

 private:
  std::vector<char> space_ = std::vector<char>(1 << 20);
  std::vector<char> captured_;
  std::size_t cursor_ = 0;
  std::size_t claimed_ = 0;
  std::size_t claimedLength_ = 0;
  std::uint64_t publishes_ = 0;
};

// Plays the sequencer: every command gets the next sequence number and a timestamp that is a pure
// function of it, so replays reproduce both (P-3).
class CommandWriter {
 public:
  struct Framed {
    std::vector<char> bytes;
  };

  std::uint64_t nextSequence() const { return sequence_ + 1; }

  Framed instrument(const std::uint32_t instrumentId, const std::int64_t tick,
                    const std::int64_t lot, const std::int64_t minPrice,
                    const std::int64_t maxPrice, const std::int64_t band, const std::int64_t open,
                    const bool proRata) {
    Framed framed = start<sbe::InstrumentDefinition>(instrumentId);
    sbe::InstrumentDefinition encoder;
    wrap(encoder, framed, instrumentId);
    encoder.tickSize(tick)
        .lotSize(lot)
        .minPrice(minPrice)
        .maxPrice(maxPrice)
        .bandWidth(band)
        .openingReference(open);
    encoder.priceScale(4).allocation(proRata ? sbe::Allocation::PRO_RATA
                                             : sbe::Allocation::PRICE_TIME);
    return framed;
  }

  Framed newOrder(const std::uint32_t instrumentId, const std::uint64_t clientOrderId,
                  const std::uint32_t participantId, const sbe::Side::Value side,
                  const sbe::Pricing::Value pricing, const sbe::TimeInForce::Value timeInForce,
                  const bool postOnly, const std::int64_t price, const std::int64_t quantity,
                  const std::int64_t minQuantity, const std::int64_t displayQuantity,
                  const std::int64_t triggerPrice, const std::uint64_t smpId,
                  const bool auctionOnly = false) {
    Framed framed = start<sbe::NewOrder>(instrumentId);
    sbe::NewOrder encoder;
    wrap(encoder, framed, instrumentId);
    encoder.clientOrderId(clientOrderId)
        .price(price)
        .quantity(quantity)
        .minQuantity(minQuantity)
        .displayQuantity(displayQuantity)
        .triggerPrice(triggerPrice)
        .smpId(smpId)
        .participantId(participantId);
    encoder.side(side).pricing(pricing).timeInForce(timeInForce);
    encoder.flags().clear().postOnly(postOnly).auctionOnly(auctionOnly);
    return framed;
  }

  Framed cancel(const std::uint32_t instrumentId, const std::uint64_t clientOrderId,
                const std::uint32_t participantId) {
    Framed framed = start<sbe::CancelOrder>(instrumentId);
    sbe::CancelOrder encoder;
    wrap(encoder, framed, instrumentId);
    encoder.clientOrderId(clientOrderId).participantId(participantId);
    return framed;
  }

  Framed replace(const std::uint32_t instrumentId, const std::uint64_t clientOrderId,
                 const std::uint32_t participantId, const std::int64_t quantity,
                 const std::int64_t price) {
    Framed framed = start<sbe::ReplaceOrder>(instrumentId);
    sbe::ReplaceOrder encoder;
    wrap(encoder, framed, instrumentId);
    encoder.clientOrderId(clientOrderId)
        .quantity(quantity)
        .price(price)
        .participantId(participantId);
    return framed;
  }

  Framed massCancel(const std::uint32_t instrumentId, const std::uint64_t clientOrderId,
                    const std::uint32_t participantId) {
    Framed framed = start<sbe::MassCancel>(instrumentId);
    sbe::MassCancel encoder;
    wrap(encoder, framed, instrumentId);
    encoder.clientOrderId(clientOrderId).participantId(participantId);
    return framed;
  }

  Framed session(const std::uint32_t instrumentId, const sbe::SessionState::Value state) {
    Framed framed = start<sbe::SessionControl>(instrumentId);
    sbe::SessionControl encoder;
    wrap(encoder, framed, instrumentId);
    encoder.state(state);
    return framed;
  }

 private:
  template <typename Encoder>
  Framed start(const std::uint32_t) {
    Framed framed;
    framed.bytes.resize(sbe::MessageHeader::encodedLength() + Encoder::sbeBlockLength());
    return framed;
  }

  template <typename Encoder>
  void wrap(Encoder& encoder, Framed& framed, const std::uint32_t instrumentId) {
    encoder.wrapAndApplyHeader(framed.bytes.data(), 0, framed.bytes.size());
    sequence_++;
    encoder.context()
        .sequence(sequence_)
        .timestamp(1'000'000'000'000ULL + sequence_)
        .instrumentId(instrumentId)
        .reserved(0);
  }

  std::uint64_t sequence_ = 0;
};

// One decoded event, flattened so tests can compare and render without touching decoders again.
struct EventView {
  std::uint16_t templateId = 0;
  std::uint64_t sequence = 0;
  std::uint64_t inputSequence = 0;
  std::uint64_t timestamp = 0;
  std::uint32_t instrumentId = 0;
  std::uint64_t orderId = 0;
  std::uint64_t clientOrderId = 0;
  std::uint32_t participantId = 0;
  std::uint64_t executionId = 0;
  std::uint64_t aggressorOrderId = 0;
  std::uint64_t restingOrderId = 0;
  std::int64_t price = 0;
  std::int64_t quantity = 0;
  std::int32_t side = 0;
  std::int32_t reason = 0;
  std::int32_t state = 0;
};

std::vector<EventView> readEvents(const std::vector<char>& bytes);
std::string render(const EventView& event);

}  // namespace exchange::matcher::test
