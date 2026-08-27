// The gate's corpus: every check refusing with its own reason, in the order the protocol names,
// cheapest first; the collar learning its reference from executions; the market order that
// cannot be bounded; and the messages that reduce risk passing everything but the throttle.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "clock.hpp"
#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/MassCancel.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/ReplaceOrder.h"
#include "harness.hpp"
#include "risk.hpp"

using namespace exchange::risk;
using exchange::sequencer::VirtualClock;
namespace sbe = exchange::protocol;

namespace {

Intent newOrder(const std::uint64_t clientOrderId, const std::int64_t price,
                const std::int64_t quantity, const bool market = false) {
  return {sbe::NewOrder::sbeTemplateId(), clientOrderId, 1, price, quantity, true, market};
}

Intent replace(const std::uint64_t clientOrderId, const std::int64_t price,
               const std::int64_t quantity) {
  return {sbe::ReplaceOrder::sbeTemplateId(), clientOrderId, 1, price, quantity, false, false};
}

Intent cancel(const std::uint64_t clientOrderId) {
  return {sbe::CancelOrder::sbeTemplateId(), clientOrderId, 1, 0, 0, false, false};
}

// A forged execution event teaches the reference without a whole venue in the room.
std::vector<char> executionAt(const std::int64_t price) {
  char space[128] = {};
  sbe::OrderExecuted event;
  event.wrapAndApplyHeader(space, 0, sizeof space);
  event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
  event.executionId(1).aggressorOrderId(900).restingOrderId(901).price(price).quantity(1);
  return std::vector<char>(
      space, space + sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
}

}  // namespace

TEST_CASE("each check refuses with its own reason, cheapest first") {
  VirtualClock clock;
  Limits limits;
  limits.maxQuantity = 100;
  limits.maxNotional = 100'000;
  limits.credit = 250'000;
  limits.collarWidth = 50;
  Risk<VirtualClock> risk(clock, {{7, limits}});

  // Size, then notional, before anything about the book is known.
  CHECK(risk.admit(7, newOrder(1, 1000, 101)).reason == sbe::RiskRefusal::MAX_ORDER_SIZE);
  CHECK(risk.admit(7, newOrder(1, 2000, 51)).reason == sbe::RiskRefusal::MAX_NOTIONAL);

  // Admitted, and a duplicate of a living clientOrderId is refused.
  CHECK(risk.admit(7, newOrder(1, 1000, 50)).admitted);
  CHECK(risk.admit(7, newOrder(1, 1000, 10)).reason == sbe::RiskRefusal::DUPLICATE);

  // The collar sleeps until a reference exists, then bites on distance.
  CHECK(risk.admit(7, newOrder(2, 5000, 10)).admitted);
  std::vector<char> taught = executionAt(1000);
  risk.onEvent(taught.data(), taught.size());
  CHECK(risk.admit(7, newOrder(3, 1051, 10)).reason == sbe::RiskRefusal::PRICE_COLLAR);
  CHECK(risk.admit(7, newOrder(3, 1050, 10)).admitted);

  // Credit: the ledger holds 110500 across the three admitted orders; 100000 more still fits
  // under 250000, then 40000 breaches, then a small one fits again.
  CHECK(risk.admit(7, newOrder(4, 1000, 100)).admitted);
  CHECK(risk.admit(7, newOrder(5, 1000, 40)).reason == sbe::RiskRefusal::CREDIT);
  CHECK(risk.admit(7, newOrder(5, 995, 10)).admitted);

  // Risk-reducing messages pass even with the credit exhausted.
  CHECK(risk.admit(7, cancel(1)).admitted);
}

TEST_CASE("the throttle is first, spends per message, and refills with time") {
  VirtualClock clock;
  Limits limits;
  limits.ratePerSecond = 10;
  limits.burst = 3;
  Risk<VirtualClock> risk(clock, {{7, limits}});

  CHECK(risk.admit(7, cancel(1)).admitted);
  CHECK(risk.admit(7, cancel(2)).admitted);
  CHECK(risk.admit(7, cancel(3)).admitted);
  // The bucket is empty, and even an oversized order reports the throttle, cheapest first.
  const Verdict fourth = risk.admit(7, newOrder(9, 1'000'000'000, 1'000'000'000));
  CHECK(!fourth.admitted);
  CHECK(fourth.reason == sbe::RiskRefusal::RATE_LIMIT);

  // A tenth of a second buys one message at ten per second.
  clock.advance(100'000'000ULL);
  CHECK(risk.admit(7, cancel(4)).admitted);
  CHECK(risk.admit(7, cancel(5)).reason == sbe::RiskRefusal::RATE_LIMIT);
}

TEST_CASE("a market order reserves at the reference and cannot be bounded without one") {
  VirtualClock clock;
  Limits limits;
  limits.credit = 100'000;
  Risk<VirtualClock> risk(clock, {{7, limits}});

  CHECK(risk.admit(7, newOrder(1, 0, 10, true)).reason == sbe::RiskRefusal::CREDIT);
  std::vector<char> taught = executionAt(2000);
  risk.onEvent(taught.data(), taught.size());
  CHECK(risk.admit(7, newOrder(1, 0, 10, true)).admitted);
  CHECK(risk.exposure(7) == 20'000);
}

TEST_CASE("a replace re-prices the reservation and a refused one rolls back") {
  VirtualClock clock;
  Limits limits;
  limits.credit = 100'000;
  Risk<VirtualClock> risk(clock, {{7, limits}});

  CHECK(risk.admit(7, newOrder(1, 1000, 50)).admitted);
  CHECK(risk.exposure(7) == 50'000);

  // Growing past credit is refused and the reservation stands.
  CHECK(risk.admit(7, replace(1, 1000, 101)).reason == sbe::RiskRefusal::CREDIT);
  CHECK(risk.exposure(7) == 50'000);

  // Growing within credit re-prices immediately.
  CHECK(risk.admit(7, replace(1, 900, 80)).admitted);
  CHECK(risk.exposure(7) == 72'000);
}
