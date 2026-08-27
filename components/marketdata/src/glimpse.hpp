// Snapshot-then-join, Glimpse's shape (docs/PROTOCOL.md, the public feed): a late joiner
// receives, per instrument, the trading state and then every visible order in queue priority
// order, so replaying the snapshot rebuilds the book's fairness as well as its shape, and
// finally SnapshotComplete naming the live sequence to continue at. The snapshot is the cold
// path and buys its simplicity with a sort.

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "builder.hpp"
#include "exchange_protocol/PublicOrderAdded.h"
#include "exchange_protocol/PublicSessionState.h"
#include "exchange_protocol/SnapshotComplete.h"
#include "ranges.hpp"

namespace exchange::marketdata {

// Serves the current book as ranges through send(bytes, length); joinAt names where the live
// feed continues, which the publisher's next() is.
template <typename Builder, typename Sink>
void snapshot(const Builder& builder, const std::uint64_t joinAt, Sink&& send) {
  char packet[common::ranges::PACKET_BYTES];
  common::ranges::Builder ranges(packet, sizeof packet);
  std::uint64_t sequence = 1;
  const auto emit = [&](const char* message, const std::size_t length) {
    if (ranges.isOpen() && !ranges.fits(length)) {
      send(packet, ranges.close());
    }
    if (!ranges.isOpen()) {
      ranges.open(sequence, 1);
    }
    ranges.add(message, static_cast<std::uint16_t>(length));
    sequence++;
  };

  std::vector<PublicOrder> orders;
  builder.forEachOrder([&](const PublicOrder& order) { orders.push_back(order); });
  std::sort(orders.begin(), orders.end(), [](const PublicOrder& a, const PublicOrder& b) {
    if (a.instrumentId != b.instrumentId) {
      return a.instrumentId < b.instrumentId;
    }
    if (a.side != b.side) {
      return a.side < b.side;
    }
    if (a.price != b.price) {
      return a.side == 0 ? a.price > b.price : a.price < b.price;
    }
    return a.arrival < b.arrival;
  });

  for (const std::uint32_t instrument : builder.instruments()) {
    char space[64];
    sbe::PublicSessionState state;
    state.wrapAndApplyHeader(space, 0, sizeof space);
    state.context().timestamp(builder.lastTimestamp()).instrumentId(instrument).reserved(0);
    state.state(builder.stateOf(instrument));
    emit(space, sbe::MessageHeader::encodedLength() + sbe::PublicSessionState::sbeBlockLength());
    for (const PublicOrder& order : orders) {
      if (order.instrumentId != instrument) {
        continue;
      }
      char added[64];
      sbe::PublicOrderAdded message;
      message.wrapAndApplyHeader(added, 0, sizeof added);
      message.context().timestamp(builder.lastTimestamp()).instrumentId(instrument).reserved(0);
      message.orderId(order.id).price(order.price).quantity(order.quantity);
      message.side(order.side == 0 ? sbe::Side::BUY : sbe::Side::SELL);
      emit(added, sbe::MessageHeader::encodedLength() + sbe::PublicOrderAdded::sbeBlockLength());
    }
  }

  char last[64];
  sbe::SnapshotComplete complete;
  complete.wrapAndApplyHeader(last, 0, sizeof last);
  complete.nextSequence(joinAt)
      .instruments(static_cast<std::uint32_t>(builder.instruments().size()))
      .reserved(0);
  emit(last, sbe::MessageHeader::encodedLength() + sbe::SnapshotComplete::sbeBlockLength());
  if (ranges.isOpen()) {
    send(packet, ranges.close());
  }
}

}  // namespace exchange::marketdata
