// The session-level risk controls, held from every angle: an unclean death sweeps a
// cancel-on-disconnect participant's books venue wide through the real carrier, a clean logout
// leaves them standing, the kill switch ends the session, refuses the door with its own word and
// sweeps exactly once, a revived participant may return, a venue-wide sweep really empties every
// book, and the ownership table breathes through a day that used to be its hard stop.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "flow.hpp"
#include "gateway.hpp"
#include "harness.hpp"
#include "partition.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;
namespace sbe = exchange::protocol;

namespace {

using Gate = exchange::risk::Risk<VirtualClock>;
using Machine = Gateway<CapturingLink, VirtualClock, Gate>;

struct Wired {
  CapturingLink submissions;
  VirtualClock clock;
  Gate risk{clock, {{7, {}}, {8, {}}}};
  // Participant 7 bought cancel on disconnect; participant 8 did not.
  Machine gateway{submissions, clock, risk, 1, {{7, 42, true}, {8, 43}}};

  int establish(const std::uint32_t participant, const std::uint64_t secret) {
    const int slot = gateway.opened();
    std::vector<char> login = loginBytes(participant, secret, 0);
    gateway.received(slot, login.data(), login.size());
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
    return slot;
  }

  // The last record the carrier took, decoded as the sweep it should be.
  void expectSweep(const std::uint32_t participant) {
    REQUIRE(!submissions.ranges.empty());
    std::vector<char>& record = submissions.ranges.back();
    char* command = record.data() + exchange::sequencer::SUBMISSION_BYTES;
    sbe::MessageHeader wrap;
    wrap.wrap(command, 0, 0, record.size());
    REQUIRE(wrap.templateId() == sbe::MassCancel::sbeTemplateId());
    sbe::MassCancel sweep;
    sweep.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        record.size());
    CHECK(sweep.context().instrumentId() == 0);
    CHECK(sweep.participantId() == participant);
  }
};

}  // namespace

TEST_CASE("an unclean death sweeps the books, and a clean logout leaves them standing") {
  Wired wired;

  // The transport drops mid-session: one venue-wide sweep under the participant's identity.
  int slot = wired.establish(7, 42);
  wired.gateway.closed(slot);
  CHECK(wired.gateway.swept() == 1);
  wired.expectSweep(7);

  // Heartbeat death is the same death.
  slot = wired.establish(7, 42);
  wired.clock.advance(6'000'000'000ULL);
  wired.gateway.onTick();
  CHECK(wired.gateway.swept() == 2);
  wired.gateway.closed(slot);
  CHECK(wired.gateway.swept() == 2);

  // A poisoned stream is an unclean death too.
  slot = wired.establish(7, 42);
  const char garbage[6] = {4, 0, 'j', 'u', 'n', 'k'};
  wired.gateway.received(slot, garbage, sizeof garbage);
  CHECK(wired.gateway.swept() == 3);
  wired.gateway.closed(slot);

  // The asked-for logout is the one orderly exit: the books stand.
  slot = wired.establish(7, 42);
  std::vector<char> logout = logoutBytes();
  wired.gateway.received(slot, logout.data(), logout.size());
  wired.gateway.closed(slot);
  CHECK(wired.gateway.swept() == 3);

  // A participant that never bought the service is never swept.
  slot = wired.establish(8, 43);
  wired.gateway.closed(slot);
  CHECK(wired.gateway.swept() == 3);
}

TEST_CASE("the kill switch ends the session, refuses the door, and sweeps exactly once") {
  Wired wired;
  const int slot = wired.establish(8, 43);
  wired.gateway.kill(8);
  CHECK(wired.gateway.kills() == 1);
  CHECK(wired.gateway.swept() == 1);
  wired.expectSweep(8);

  // The session heard its last word and the connection wants to die.
  const auto [bytes, length] = wired.gateway.outbound(slot);
  const auto messages = unframed(bytes, length);
  wired.gateway.drained(slot, length);
  REQUIRE(!messages.empty());
  CHECK(messages.back().first == sbe::SessionEnded::sbeTemplateId());
  CHECK(wired.gateway.wantsClose(slot));
  wired.gateway.closed(slot);

  // Killing the dead is a no-op; the door answers with the kill's own word.
  wired.gateway.kill(8);
  CHECK(wired.gateway.kills() == 1);
  CHECK(wired.gateway.swept() == 1);
  const int refused = wired.gateway.opened();
  std::vector<char> login = loginBytes(8, 43, 0);
  wired.gateway.received(refused, login.data(), login.size());
  const auto [answer, answerLength] = wired.gateway.outbound(refused);
  const auto refusal = unframed(answer, answerLength);
  REQUIRE(refusal.size() == 1);
  REQUIRE(refusal[0].first == sbe::LoginRejected::sbeTemplateId());
  sbe::MessageHeader wrap;
  std::vector<char> copy = refusal[0].second;
  wrap.wrap(copy.data(), 0, 0, copy.size());
  sbe::LoginRejected rejected;
  rejected.wrapForDecode(copy.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                         copy.size());
  CHECK(rejected.reason() == sbe::LoginRefusal::KILLED);
  wired.gateway.closed(refused);

  // Revival reopens the door and nothing more.
  wired.gateway.revive(8);
  const int back = wired.establish(8, 43);
  CHECK(!wired.gateway.wantsClose(back));
  CHECK(wired.gateway.swept() == 1);
}

TEST_CASE("a mass cancel naming instrument zero sweeps every book") {
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  CommandWriter writer;
  const auto play = [&](const CommandWriter::Framed& command) {
    std::vector<char> bytes = command.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  };
  play(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  play(writer.instrument(2, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  play(writer.session(1, sbe::SessionState::CONTINUOUS));
  play(writer.session(2, sbe::SessionState::CONTINUOUS));
  play(writer.newOrder(1, 10, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 10, 0, 0, 0, 0));
  play(writer.newOrder(2, 11, 7, sbe::Side::SELL, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'010, 10, 0, 0, 0, 0));
  play(writer.newOrder(1, 12, 8, sbe::Side::BUY, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::GOOD_TILL_CANCEL, false, 99'995, 5, 0, 0, 0, 0));

  play(writer.massCancel(0, 0, 7));

  // Participant 7's orders left both books; participant 8's order stands.
  std::size_t removals = 0;
  std::size_t at = 0;
  const std::vector<char>& seen = events.captured();
  std::vector<char> copy(seen);
  while (at < copy.size()) {
    sbe::MessageHeader wrap;
    wrap.wrap(copy.data(), at, 0, copy.size());
    const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
    if (wrap.templateId() == sbe::OrderRemoved::sbeTemplateId()) {
      sbe::OrderRemoved removed;
      removed.wrapForDecode(copy.data(), at + sbe::MessageHeader::encodedLength(),
                            wrap.blockLength(), wrap.version(), copy.size());
      CHECK(removed.reason() == sbe::RemoveReason::MASS_CANCELLED);
      removals++;
    }
    at += length;
  }
  CHECK(removals == 2);
  CHECK(partition.engine(1).book().restingSlots().size() == 1);
  CHECK(partition.engine(2).book().restingSlots().empty());
}

TEST_CASE("the ownership table breathes through the day that used to be its hard stop") {
  Wired wired;
  // Three hundred thousand aggressors that filled whole, no removal ever naming them: the old
  // table threw at three quarters full, and a table that never forgets probes forever.
  for (std::uint64_t orderId = 1; orderId <= 300'000; orderId++) {
    std::vector<char> event = acceptedEvent(orderId, 99, orderId);
    wired.gateway.onEvent(event.data(), event.size());
  }
  // The freshest order is known; the oldest aged out by orderId distance.
  std::vector<char> young = executedEvent(300'000, 300'000);
  wired.gateway.onEvent(young.data(), young.size());
  CHECK(wired.gateway.swept() == 0);
}
