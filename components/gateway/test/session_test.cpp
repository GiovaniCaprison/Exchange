// The session's law, held from the outside: logins are checked and typed refusals close the
// connection; identity is written by the gateway whatever the client claimed; the vocabulary is
// the permission and speaking outside it ends the session; heartbeats pulse and silence kills;
// and a reconnection replays the participant's stream from the sequence it names, byte exactly.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "flow.hpp"
#include "gateway.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using exchange::matcher::test::generatedFlow;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;

namespace {

using Gate = exchange::risk::Risk<VirtualClock>;
using Machine = Gateway<CapturingLink, VirtualClock, Gate>;

struct Wired {
  CapturingLink submissions;
  VirtualClock clock;
  Gate risk{clock, {{7, {}}, {8, {}}}};
  Machine gateway{submissions, clock, risk, 1, {{7, 42}, {8, 43}}};

  std::vector<std::pair<std::uint16_t, std::vector<char>>> read(const int slot) {
    const auto [bytes, length] = gateway.outbound(slot);
    const auto messages = unframed(bytes, length);
    gateway.drained(slot, length);
    return messages;
  }
};

}  // namespace

TEST_CASE("a login is answered and a wrong one is refused with its reason") {
  Wired wired;
  const int good = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(good, login.data(), login.size());
  auto messages = wired.read(good);
  REQUIRE(messages.size() == 1);
  CHECK(messages[0].first == exchange::protocol::LoginAccepted::sbeTemplateId());
  CHECK(!wired.gateway.wantsClose(good));

  const struct {
    std::uint32_t participant;
    std::uint64_t secret;
    std::uint64_t expected;
  } refusals[] = {{9, 42, 0}, {8, 99, 0}, {7, 42, 0}, {8, 43, 5}};
  for (const auto& attempt : refusals) {
    const int bad = wired.gateway.opened();
    std::vector<char> ask = loginBytes(attempt.participant, attempt.secret, attempt.expected);
    wired.gateway.received(bad, ask.data(), ask.size());
    auto answer = wired.read(bad);
    REQUIRE(answer.size() == 1);
    CHECK(answer[0].first == exchange::protocol::LoginRejected::sbeTemplateId());
    CHECK(wired.gateway.wantsClose(bad));
    wired.gateway.closed(bad);
  }
  CHECK(wired.gateway.rejections() == 4);
}

TEST_CASE("identity is the session's fact, whatever the client wrote") {
  Wired wired;
  const int slot = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(slot, login.data(), login.size());
  // The client claims to be participant 5 inside the order; the session is participant 7.
  CommandWriter writer;
  std::vector<char> order = commandBytes(writer.newOrder(
      1, 900, 5, exchange::protocol::Side::BUY, exchange::protocol::Pricing::LIMIT,
      exchange::protocol::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 10, 0, 0, 0, 0));
  wired.gateway.received(slot, order.data(), order.size());

  REQUIRE(wired.submissions.ranges.size() == 1);
  std::vector<char>& record = wired.submissions.ranges[0];
  namespace sbe = exchange::protocol;
  sbe::MessageHeader wrap;
  wrap.wrap(record.data(), 0, 0, record.size());
  sbe::GatewaySubmission envelope;
  envelope.wrapForDecode(record.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                         record.size());
  CHECK(envelope.gatewaySequence() == 1);
  CHECK(envelope.gatewayId() == 1);
  char* command = record.data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader inner;
  inner.wrap(command, 0, 0, record.size());
  sbe::NewOrder decoded;
  decoded.wrapForDecode(command, inner.encodedLength(), inner.blockLength(), inner.version(),
                        record.size());
  CHECK(decoded.participantId() == 7);
  CHECK(decoded.clientOrderId() == 900);
}

TEST_CASE("bytes arrive one at a time and the frames still assemble") {
  Wired wired;
  const int slot = wired.gateway.opened();
  std::vector<char> stream = loginBytes(7, 42, 0);
  CommandWriter writer;
  std::vector<char> order = commandBytes(writer.newOrder(
      1, 901, 7, exchange::protocol::Side::SELL, exchange::protocol::Pricing::LIMIT,
      exchange::protocol::TimeInForce::GOOD_TILL_CANCEL, false, 1010, 5, 0, 0, 0, 0));
  stream.insert(stream.end(), order.begin(), order.end());
  for (const char byte : stream) {
    wired.gateway.received(slot, &byte, 1);
  }
  CHECK(wired.gateway.submitted() == 1);
  CHECK(wired.gateway.poisoned() == 0);
}

TEST_CASE("the vocabulary is the permission") {
  Wired wired;
  // A command before login is not a client.
  const int early = wired.gateway.opened();
  CommandWriter writer;
  std::vector<char> order = commandBytes(writer.newOrder(
      1, 902, 7, exchange::protocol::Side::BUY, exchange::protocol::Pricing::LIMIT,
      exchange::protocol::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 1, 0, 0, 0, 0));
  wired.gateway.received(early, order.data(), order.size());
  CHECK(wired.gateway.poisoned() == 1);
  CHECK(wired.gateway.submitted() == 0);

  // Session control from a client is the operations component being impersonated.
  const int slot = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(slot, login.data(), login.size());
  std::vector<char> forbidden =
      commandBytes(writer.session(1, exchange::protocol::SessionState::HALTED));
  wired.gateway.received(slot, forbidden.data(), forbidden.size());
  CHECK(wired.gateway.poisoned() == 2);
  CHECK(wired.gateway.submitted() == 0);
}

TEST_CASE("heartbeats pulse out on idle and silence in ends the session") {
  Wired wired;
  const int slot = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(slot, login.data(), login.size());
  wired.read(slot);

  wired.clock.advance(1'100'000'000ULL);
  wired.gateway.onTick();
  auto pulses = wired.read(slot);
  REQUIRE(pulses.size() == 1);
  CHECK(pulses[0].first == exchange::protocol::SessionHeartbeat::sbeTemplateId());

  // The client's own heartbeats keep it alive; then silence past the allowance kills it.
  std::vector<char> pulse = heartbeatBytes();
  wired.gateway.received(slot, pulse.data(), pulse.size());
  wired.clock.advance(4'000'000'000ULL);
  wired.gateway.onTick();
  CHECK(wired.gateway.timeouts() == 0);
  wired.clock.advance(6'000'000'000ULL);
  wired.gateway.onTick();
  CHECK(wired.gateway.timeouts() == 1);
  auto last = wired.read(slot);
  REQUIRE(!last.empty());
  CHECK(last.back().first == exchange::protocol::SessionEnded::sbeTemplateId());
  CHECK(wired.gateway.wantsClose(slot));
}

TEST_CASE("a logout ends the session politely") {
  Wired wired;
  const int slot = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(slot, login.data(), login.size());
  std::vector<char> logout = logoutBytes();
  wired.gateway.received(slot, logout.data(), logout.size());
  auto messages = wired.read(slot);
  REQUIRE(messages.size() == 2);
  CHECK(messages[1].first == exchange::protocol::SessionEnded::sbeTemplateId());
  CHECK(wired.gateway.wantsClose(slot));
}

TEST_CASE("a reconnection replays the stream from the sequence it names, byte exactly") {
  Wired wired;
  const int first = wired.gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  wired.gateway.received(first, login.data(), login.size());

  std::vector<std::vector<char>> events;
  for (std::uint64_t at = 1; at <= 3; at++) {
    events.push_back(acceptedEvent(at, 7, 900 + at));
    wired.gateway.onEvent(events.back().data(), events.back().size());
  }
  const auto seen = wired.read(first);
  REQUIRE(seen.size() == 4);
  wired.gateway.closed(first);

  // The client saw sequences 1 and 2 before it died, so it asks for 3.
  const int second = wired.gateway.opened();
  std::vector<char> back = loginBytes(7, 42, 3);
  wired.gateway.received(second, back.data(), back.size());
  const auto replayed = wired.read(second);
  REQUIRE(replayed.size() == 2);
  CHECK(replayed[0].first == exchange::protocol::LoginAccepted::sbeTemplateId());
  CHECK(replayed[1].second == events[2]);
}
