// The gate proven where it stands: inside the gateway, refusing on the session with the typed
// reason, and nothing refused ever reaching the submission ring; what the gate admits flows
// exactly as before.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "exchange_protocol/CommandRefused.h"
#include "gateway.hpp"
#include "risk.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using exchange::risk::Limits;
using exchange::risk::Risk;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;
namespace sbe = exchange::protocol;

TEST_CASE("the gate refuses on the session and the venue never hears about it") {
  CapturingLink submissions;
  VirtualClock clock;
  Limits tight;
  tight.maxQuantity = 10;
  Risk<VirtualClock> risk(clock, {{7, tight}});
  Gateway<CapturingLink, VirtualClock, Risk<VirtualClock>> gateway(submissions, clock, risk, 1,
                                                                   {{7, 42}});
  const int slot = gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size());
  {
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
  }

  CommandWriter writer;
  std::vector<char> oversized = commandBytes(
      writer.newOrder(1, 900, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 11, 0, 0, 0, 0));
  gateway.received(slot, oversized.data(), oversized.size());

  CHECK(submissions.ranges.empty());
  CHECK(gateway.gateRefusals() == 1);
  const auto [bytes, length] = gateway.outbound(slot);
  const auto messages = unframed(bytes, length);
  REQUIRE(messages.size() == 1);
  CHECK(messages[0].first == sbe::CommandRefused::sbeTemplateId());
  sbe::MessageHeader wrap;
  std::vector<char> answer = messages[0].second;
  wrap.wrap(answer.data(), 0, 0, answer.size());
  sbe::CommandRefused refused;
  refused.wrapForDecode(answer.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        answer.size());
  CHECK(refused.clientOrderId() == 900);
  CHECK(refused.reason() == sbe::RiskRefusal::MAX_ORDER_SIZE);
  gateway.drained(slot, length);

  // What the gate admits flows exactly as before, and a cancel passes a tight gate.
  std::vector<char> fitting = commandBytes(
      writer.newOrder(1, 901, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 10, 0, 0, 0, 0));
  gateway.received(slot, fitting.data(), fitting.size());
  std::vector<char> sweep = commandBytes(writer.cancel(1, 901, 7));
  gateway.received(slot, sweep.data(), sweep.size());
  CHECK(submissions.ranges.size() == 2);
  CHECK(gateway.submitted() == 2);
}
