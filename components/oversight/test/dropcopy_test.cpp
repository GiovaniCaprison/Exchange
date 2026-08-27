// Drop copy held to the session law from the watcher's chair: logins are checked and typed
// refusals close the connection, a watcher hears its scope's events byte exactly and nobody
// else's, a reconnection replays from the sequence named, and a command poisons the session,
// because the channel exists so the client cannot touch it.

#include "dropcopy.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "flow.hpp"

using namespace exchange::oversight;
using exchange::gateway::test::acceptedEvent;
using exchange::gateway::test::commandBytes;
using exchange::gateway::test::executedEvent;
using exchange::gateway::test::heartbeatBytes;
using exchange::gateway::test::loginBytes;
using exchange::gateway::test::unframed;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::VirtualClock;
namespace sbe = exchange::protocol;

namespace {

using Machine = DropCopy<VirtualClock>;

struct Wired {
  VirtualClock clock;
  Machine machine{clock, {{7, 90'042}, {8, 90'043}}};

  std::vector<std::pair<std::uint16_t, std::vector<char>>> read(const int slot) {
    const auto [bytes, length] = machine.outbound(slot);
    const auto messages = unframed(bytes, length);
    machine.drained(slot, length);
    return messages;
  }

  int watch(const std::uint32_t scope, const std::uint64_t secret,
            const std::uint64_t expected = 0) {
    const int slot = machine.opened();
    std::vector<char> login = loginBytes(scope, secret, expected);
    machine.received(slot, login.data(), login.size());
    return slot;
  }
};

}  // namespace

TEST_CASE("a watcher hears its scope and nobody else, byte exactly") {
  Wired wired;
  const int slot = wired.watch(7, 90'042);
  auto messages = wired.read(slot);
  REQUIRE(messages.size() == 1);
  CHECK(messages[0].first == sbe::LoginAccepted::sbeTemplateId());

  // Participant 7's acceptance, participant 8's acceptance, and an execution between them.
  std::vector<char> sevens = acceptedEvent(900, 7, 1);
  std::vector<char> eights = acceptedEvent(901, 8, 2);
  std::vector<char> print = executedEvent(901, 900);
  wired.machine.onEvent(sevens.data(), sevens.size());
  wired.machine.onEvent(eights.data(), eights.size());
  wired.machine.onEvent(print.data(), print.size());

  messages = wired.read(slot);
  REQUIRE(messages.size() == 2);
  CHECK(messages[0].first == sbe::OrderAccepted::sbeTemplateId());
  CHECK(messages[0].second == sevens);
  CHECK(messages[1].first == sbe::OrderExecuted::sbeTemplateId());
  CHECK(messages[1].second == print);
}

TEST_CASE("logins are checked, and the refusals are typed") {
  Wired wired;
  const struct {
    std::uint32_t scope;
    std::uint64_t secret;
    std::uint64_t expected;
    sbe::LoginRefusal::Value reason;
  } refusals[] = {{9, 90'042, 0, sbe::LoginRefusal::UNKNOWN_PARTICIPANT},
                  {7, 1, 0, sbe::LoginRefusal::BAD_CREDENTIAL},
                  {7, 90'042, 50, sbe::LoginRefusal::SEQUENCE_AHEAD}};
  for (const auto& attempt : refusals) {
    const int slot = wired.watch(attempt.scope, attempt.secret, attempt.expected);
    auto messages = wired.read(slot);
    REQUIRE(messages.size() == 1);
    REQUIRE(messages[0].first == sbe::LoginRejected::sbeTemplateId());
    sbe::MessageHeader wrap;
    wrap.wrap(messages[0].second.data(), 0, 0, messages[0].second.size());
    sbe::LoginRejected rejected;
    rejected.wrapForDecode(messages[0].second.data(), wrap.encodedLength(), wrap.blockLength(),
                           wrap.version(), messages[0].second.size());
    CHECK(rejected.reason() == attempt.reason);
    CHECK(wired.machine.wantsClose(slot));
    wired.machine.closed(slot);
  }

  // One scope, one seat: a second login for a bound stream is refused.
  const int first = wired.watch(7, 90'042);
  wired.read(first);
  const int second = wired.watch(7, 90'042);
  auto messages = wired.read(second);
  REQUIRE(messages.size() == 1);
  CHECK(messages[0].first == sbe::LoginRejected::sbeTemplateId());
  CHECK(wired.machine.rejections() == 4);
}

TEST_CASE("a reconnection replays from the sequence named, byte exactly") {
  Wired wired;
  std::vector<std::vector<char>> story;
  story.push_back(acceptedEvent(900, 7, 1));
  story.push_back(executedEvent(901, 900));
  for (std::vector<char>& event : story) {
    wired.machine.onEvent(event.data(), event.size());
  }

  // A watcher arriving late asks from the start and hears the whole story.
  const int slot = wired.watch(7, 90'042);
  auto messages = wired.read(slot);
  REQUIRE(messages.size() == 3);
  CHECK(messages[1].second == story[0]);
  CHECK(messages[2].second == story[1]);

  // The transport dies; the story continues; the reconnection names sequence three.
  wired.machine.closed(slot);
  std::vector<char> later = acceptedEvent(902, 7, 3);
  wired.machine.onEvent(later.data(), later.size());
  const int again = wired.watch(7, 90'042, 3);
  messages = wired.read(again);
  REQUIRE(messages.size() == 2);
  CHECK(messages[0].first == sbe::LoginAccepted::sbeTemplateId());
  CHECK(messages[1].second == later);
}

TEST_CASE("a command poisons the session, and heartbeats keep a quiet one alive") {
  Wired wired;
  const int slot = wired.watch(7, 90'042);
  wired.read(slot);

  for (int beat = 0; beat < 8; beat++) {
    wired.clock.advance(1'000'000'000ULL);
    std::vector<char> pulse = heartbeatBytes();
    wired.machine.received(slot, pulse.data(), pulse.size());
    wired.machine.onTick();
    CHECK(!wired.machine.wantsClose(slot));
    wired.read(slot);
  }

  // The plane's whole point: order entry has no place here.
  CommandWriter writer;
  std::vector<char> order = commandBytes(
      writer.newOrder(1, 900, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 10, 0, 0, 0, 0));
  wired.machine.received(slot, order.data(), order.size());
  CHECK(wired.machine.wantsClose(slot));
  CHECK(wired.machine.poisoned() == 1);
}
