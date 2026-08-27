// Every message through its encoder and back through its decoder, with the layout claims the
// protocol document makes checked where they are cheap to check: contexts are 24 and 32 bytes, and
// every 64 bit field sits on an eight byte boundary once the eight byte message header is placed
// on one.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "exchange_protocol/AuctionIndicative.h"
#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/CommandRefused.h"
#include "exchange_protocol/CommandSequenced.h"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/InstrumentDefinition.h"
#include "exchange_protocol/LeaseRequest.h"
#include "exchange_protocol/LeaseResponse.h"
#include "exchange_protocol/LoginAccepted.h"
#include "exchange_protocol/LoginRejected.h"
#include "exchange_protocol/LoginRequest.h"
#include "exchange_protocol/LogoutRequest.h"
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
#include "exchange_protocol/PublicOrderAdded.h"
#include "exchange_protocol/PublicTopOfBook.h"
#include "exchange_protocol/RangeHeader.h"
#include "exchange_protocol/ReplaceOrder.h"
#include "exchange_protocol/ReplicationAck.h"
#include "exchange_protocol/RewindRequest.h"
#include "exchange_protocol/SessionControl.h"
#include "exchange_protocol/SessionEnded.h"
#include "exchange_protocol/SessionHeartbeat.h"
#include "exchange_protocol/SessionStateChanged.h"
#include "exchange_protocol/SnapshotComplete.h"

using namespace exchange::protocol;

namespace {

template <typename Encoder>
Encoder encoded(std::vector<char>& space) {
  MessageHeader header;
  Encoder encoder;
  encoder.wrapAndApplyHeader(space.data(), 0, space.size());
  return encoder;
}

template <typename Decoder>
Decoder decoded(std::vector<char>& space) {
  MessageHeader header;
  header.wrap(space.data(), 0, 0, space.size());
  Decoder decoder;
  decoder.wrapForDecode(space.data(), MessageHeader::encodedLength(), header.blockLength(),
                        header.version(), space.size());
  return decoder;
}

}  // namespace

TEST_CASE("the contexts and carrier prefixes hold the sizes the protocol document names") {
  CHECK(CommandContext::encodedLength() == 24);
  CHECK(EventContext::encodedLength() == 32);
  CHECK(MessageHeader::encodedLength() + GatewaySubmission::sbeBlockLength() == 24);
  CHECK(MessageHeader::encodedLength() + RangeHeader::sbeBlockLength() == 24);
}

TEST_CASE("the public feed round trips and can say nothing private") {
  std::vector<char> space(256);
  {
    auto out = encoded<PublicOrderAdded>(space);
    out.context().timestamp(9).instrumentId(1).reserved(0);
    out.orderId(41).price(1000).quantity(5);
    out.side(Side::BUY);
    auto in = decoded<PublicOrderAdded>(space);
    CHECK(in.orderId() == 41);
    CHECK(in.price() == 1000);
    CHECK(in.quantity() == 5);
    CHECK(in.context().timestamp() == 9);
  }
  {
    auto out = encoded<PublicTopOfBook>(space);
    out.context().timestamp(9).instrumentId(1).reserved(0);
    out.bidPrice(995).bidQuantity(7).askPrice(1005).askQuantity(3);
    auto in = decoded<PublicTopOfBook>(space);
    CHECK(in.bidPrice() == 995);
    CHECK(in.askQuantity() == 3);
  }
  {
    auto out = encoded<SnapshotComplete>(space);
    out.nextSequence(4242).instruments(2).reserved(0);
    auto in = decoded<SnapshotComplete>(space);
    CHECK(in.nextSequence() == 4242);
    CHECK(in.instruments() == 2);
  }
  // The privacy claim is structural: the public context has no room for attribution.
  CHECK(PublicContext::encodedLength() == 16);
}

TEST_CASE("the session plane round trips") {
  std::vector<char> space(256);
  {
    auto out = encoded<CommandRefused>(space);
    out.clientOrderId(901).reason(RiskRefusal::PRICE_COLLAR);
    auto in = decoded<CommandRefused>(space);
    CHECK(in.clientOrderId() == 901);
    CHECK(in.reason() == RiskRefusal::PRICE_COLLAR);
  }
  {
    auto out = encoded<LoginRequest>(space);
    out.expectedSequence(41).credential(0xC0FFEE).participantId(7).reserved(0);
    auto in = decoded<LoginRequest>(space);
    CHECK(in.expectedSequence() == 41);
    CHECK(in.credential() == 0xC0FFEE);
    CHECK(in.participantId() == 7);
  }
  {
    auto out = encoded<LoginAccepted>(space);
    out.nextSequence(41).participantId(7).reserved(0);
    auto in = decoded<LoginAccepted>(space);
    CHECK(in.nextSequence() == 41);
    CHECK(in.participantId() == 7);
  }
  {
    auto out = encoded<LoginRejected>(space);
    out.reason(LoginRefusal::SEQUENCE_AHEAD);
    auto in = decoded<LoginRejected>(space);
    CHECK(in.reason() == LoginRefusal::SEQUENCE_AHEAD);
  }
  {
    encoded<SessionHeartbeat>(space).reserved(0);
    decoded<SessionHeartbeat>(space);
    encoded<LogoutRequest>(space).reserved(0);
    decoded<LogoutRequest>(space);
    encoded<SessionEnded>(space).reserved(0);
    decoded<SessionEnded>(space);
  }
}

TEST_CASE("the carrier prefixes round trip") {
  std::vector<char> space(256);
  {
    auto out = encoded<GatewaySubmission>(space);
    out.gatewaySequence(9).gatewayId(3).reserved(0);
    auto in = decoded<GatewaySubmission>(space);
    CHECK(in.gatewaySequence() == 9);
    CHECK(in.gatewayId() == 3);
  }
  {
    auto out = encoded<RangeHeader>(space);
    out.firstSequence(100).epoch(2).count(7).reserved(0);
    auto in = decoded<RangeHeader>(space);
    CHECK(in.firstSequence() == 100);
    CHECK(in.epoch() == 2);
    CHECK(in.count() == 7);
  }
}

TEST_CASE("the sequencing plane's messages round trip") {
  std::vector<char> space(256);
  {
    auto out = encoded<CommandSequenced>(space);
    out.gatewaySequence(9).sequence(42).timestamp(1'000'000'000'042ULL);
    auto in = decoded<CommandSequenced>(space);
    CHECK(in.gatewaySequence() == 9);
    CHECK(in.sequence() == 42);
    CHECK(in.timestamp() == 1'000'000'000'042ULL);
  }
  {
    auto out = encoded<ReplicationAck>(space);
    out.upToSequence(42).epoch(3).reserved(0);
    auto in = decoded<ReplicationAck>(space);
    CHECK(in.upToSequence() == 42);
    CHECK(in.epoch() == 3);
  }
  {
    auto out = encoded<LeaseRequest>(space);
    out.lastSequence(42).nodeId(2).epoch(4);
    auto in = decoded<LeaseRequest>(space);
    CHECK(in.lastSequence() == 42);
    CHECK(in.nodeId() == 2);
    CHECK(in.epoch() == 4);
  }
  {
    auto out = encoded<LeaseResponse>(space);
    out.ttl(500'000'000).epoch(4).holder(2).granted(1);
    auto in = decoded<LeaseResponse>(space);
    CHECK(in.ttl() == 500'000'000);
    CHECK(in.epoch() == 4);
    CHECK(in.holder() == 2);
    CHECK(in.granted() == 1);
  }
  {
    auto out = encoded<RewindRequest>(space);
    out.firstSequence(100).count(50).reserved(0);
    auto in = decoded<RewindRequest>(space);
    CHECK(in.firstSequence() == 100);
    CHECK(in.count() == 50);
  }
}

TEST_CASE("a new order round trips with every field the protocol defines") {
  std::vector<char> space(256);
  auto out = encoded<NewOrder>(space);
  out.context().sequence(42).timestamp(1'700'000'000'000'000'000ULL).instrumentId(7).reserved(0);
  out.clientOrderId(9001)
      .price(100'000)
      .quantity(300)
      .minQuantity(100)
      .displayQuantity(200)
      .triggerPrice(99'500)
      .smpId(55)
      .participantId(12);
  out.side(Side::BUY).pricing(Pricing::LIMIT).timeInForce(TimeInForce::DAY);
  out.flags().clear().postOnly(true);

  auto in = decoded<NewOrder>(space);
  CHECK(in.context().sequence() == 42);
  CHECK(in.context().timestamp() == 1'700'000'000'000'000'000ULL);
  CHECK(in.context().instrumentId() == 7);
  CHECK(in.clientOrderId() == 9001);
  CHECK(in.price() == 100'000);
  CHECK(in.quantity() == 300);
  CHECK(in.minQuantity() == 100);
  CHECK(in.displayQuantity() == 200);
  CHECK(in.triggerPrice() == 99'500);
  CHECK(in.smpId() == 55);
  CHECK(in.participantId() == 12);
  CHECK(in.side() == Side::BUY);
  CHECK(in.pricing() == Pricing::LIMIT);
  CHECK(in.timeInForce() == TimeInForce::DAY);
  CHECK(in.flags().postOnly());
}

TEST_CASE("an instrument definition round trips") {
  std::vector<char> space(256);
  auto out = encoded<InstrumentDefinition>(space);
  out.context().sequence(1).timestamp(1).instrumentId(7).reserved(0);
  out.tickSize(5).lotSize(10).minPrice(5).maxPrice(1'000'000).bandWidth(500).openingReference(
      100'000);
  out.priceScale(4).allocation(Allocation::PRO_RATA);

  auto in = decoded<InstrumentDefinition>(space);
  CHECK(in.tickSize() == 5);
  CHECK(in.lotSize() == 10);
  CHECK(in.minPrice() == 5);
  CHECK(in.maxPrice() == 1'000'000);
  CHECK(in.bandWidth() == 500);
  CHECK(in.openingReference() == 100'000);
  CHECK(in.priceScale() == 4);
  CHECK(in.allocation() == Allocation::PRO_RATA);
}

TEST_CASE("the amendment commands round trip") {
  std::vector<char> space(256);
  {
    auto out = encoded<CancelOrder>(space);
    out.context().sequence(2).timestamp(2).instrumentId(7).reserved(0);
    out.clientOrderId(9001).participantId(12);
    auto in = decoded<CancelOrder>(space);
    CHECK(in.clientOrderId() == 9001);
    CHECK(in.participantId() == 12);
  }
  {
    auto out = encoded<ReplaceOrder>(space);
    out.context().sequence(3).timestamp(3).instrumentId(7).reserved(0);
    out.clientOrderId(9001).quantity(500).price(99'995).participantId(12);
    auto in = decoded<ReplaceOrder>(space);
    CHECK(in.quantity() == 500);
    CHECK(in.price() == 99'995);
  }
  {
    auto out = encoded<MassCancel>(space);
    out.context().sequence(4).timestamp(4).instrumentId(7).reserved(0);
    out.clientOrderId(1).participantId(12);
    auto in = decoded<MassCancel>(space);
    CHECK(in.participantId() == 12);
  }
  {
    auto out = encoded<SessionControl>(space);
    out.context().sequence(5).timestamp(5).instrumentId(7).reserved(0);
    out.state(SessionState::CONTINUOUS);
    auto in = decoded<SessionControl>(space);
    CHECK(in.state() == SessionState::CONTINUOUS);
  }
}

TEST_CASE("every event carries its attribution and round trips") {
  std::vector<char> space(256);
  {
    auto out = encoded<OrderAccepted>(space);
    out.context().sequence(1).inputSequence(42).timestamp(9).instrumentId(7).reserved(0);
    out.orderId(1).clientOrderId(9001).participantId(12);
    auto in = decoded<OrderAccepted>(space);
    CHECK(in.context().sequence() == 1);
    CHECK(in.context().inputSequence() == 42);
    CHECK(in.context().timestamp() == 9);
    CHECK(in.orderId() == 1);
  }
  {
    auto out = encoded<OrderRejected>(space);
    out.context().sequence(2).inputSequence(43).timestamp(9).instrumentId(7).reserved(0);
    out.clientOrderId(9002).participantId(12).reason(RejectReason::TICK_VIOLATION);
    auto in = decoded<OrderRejected>(space);
    CHECK(in.reason() == RejectReason::TICK_VIOLATION);
  }
  {
    auto out = encoded<OrderRested>(space);
    out.context().sequence(3).inputSequence(44).timestamp(9).instrumentId(7).reserved(0);
    out.orderId(1).price(100'000).quantity(200).side(Side::SELL);
    auto in = decoded<OrderRested>(space);
    CHECK(in.price() == 100'000);
    CHECK(in.side() == Side::SELL);
  }
  {
    auto out = encoded<OrderExecuted>(space);
    out.context().sequence(4).inputSequence(45).timestamp(9).instrumentId(7).reserved(0);
    out.executionId(1).aggressorOrderId(2).restingOrderId(1).price(100'000).quantity(50);
    auto in = decoded<OrderExecuted>(space);
    CHECK(in.executionId() == 1);
    CHECK(in.aggressorOrderId() == 2);
    CHECK(in.restingOrderId() == 1);
    CHECK(in.quantity() == 50);
  }
  {
    auto out = encoded<OrderReduced>(space);
    out.context().sequence(5).inputSequence(46).timestamp(9).instrumentId(7).reserved(0);
    out.orderId(1).quantity(150);
    auto in = decoded<OrderReduced>(space);
    CHECK(in.quantity() == 150);
  }
  {
    auto out = encoded<OrderRemoved>(space);
    out.context().sequence(6).inputSequence(47).timestamp(9).instrumentId(7).reserved(0);
    out.orderId(1).quantity(150).reason(RemoveReason::CANCELLED);
    auto in = decoded<OrderRemoved>(space);
    CHECK(in.reason() == RemoveReason::CANCELLED);
  }
  {
    auto out = encoded<OrderTriggered>(space);
    out.context().sequence(7).inputSequence(48).timestamp(9).instrumentId(7).reserved(0);
    out.orderId(3);
    auto in = decoded<OrderTriggered>(space);
    CHECK(in.orderId() == 3);
  }
  {
    auto out = encoded<SessionStateChanged>(space);
    out.context().sequence(8).inputSequence(49).timestamp(9).instrumentId(7).reserved(0);
    out.state(SessionState::HALTED);
    auto in = decoded<SessionStateChanged>(space);
    CHECK(in.state() == SessionState::HALTED);
  }
  {
    auto out = encoded<AuctionIndicative>(space);
    out.context().sequence(9).inputSequence(50).timestamp(9).instrumentId(7).reserved(0);
    out.price(100'000).quantity(400);
    auto in = decoded<AuctionIndicative>(space);
    CHECK(in.price() == 100'000);
    CHECK(in.quantity() == 400);
  }
}

TEST_CASE("64 bit body fields sit on eight byte boundaries behind the header") {
  // The header is eight bytes and the contexts are 24 and 32, so the first body field of every
  // message begins at a multiple of eight, and the layouts in the schema keep the wider fields
  // ahead of the narrower ones.
  CHECK(MessageHeader::encodedLength() == 8);
  CHECK(NewOrder::clientOrderIdEncodingOffset() % 8 == 0);
  CHECK(NewOrder::priceEncodingOffset() % 8 == 0);
  CHECK(NewOrder::smpIdEncodingOffset() % 8 == 0);
  CHECK(OrderExecuted::executionIdEncodingOffset() % 8 == 0);
  CHECK(OrderExecuted::priceEncodingOffset() % 8 == 0);
  CHECK(InstrumentDefinition::tickSizeEncodingOffset() % 8 == 0);
}
