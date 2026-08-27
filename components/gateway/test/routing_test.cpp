// The report router's law: a session hears about its own orders, learned from the acceptance
// that names both the order and its owner; an execution reaches both owners once each; and what
// belongs to nobody the gateway can see is counted rather than guessed at.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "gateway.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;

namespace {

using Machine = Gateway<CapturingLink, VirtualClock>;

struct Wired {
  CapturingLink submissions;
  VirtualClock clock;
  Machine gateway{submissions, clock, 1, {{7, 42}, {8, 43}}};

  int loginAs(const std::uint32_t participant, const std::uint64_t secret) {
    const int slot = gateway.opened();
    std::vector<char> login = loginBytes(participant, secret, 0);
    gateway.received(slot, login.data(), login.size());
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
    return slot;
  }

  std::vector<std::pair<std::uint16_t, std::vector<char>>> read(const int slot) {
    const auto [bytes, length] = gateway.outbound(slot);
    const auto messages = unframed(bytes, length);
    gateway.drained(slot, length);
    return messages;
  }
};

}  // namespace

TEST_CASE("reports reach their owner and only their owner") {
  Wired wired;
  const int seven = wired.loginAs(7, 42);
  const int eight = wired.loginAs(8, 43);

  std::vector<char> mine = acceptedEvent(10, 7, 900);
  std::vector<char> theirs = acceptedEvent(20, 8, 800);
  wired.gateway.onEvent(mine.data(), mine.size());
  wired.gateway.onEvent(theirs.data(), theirs.size());

  const auto atSeven = wired.read(seven);
  const auto atEight = wired.read(eight);
  REQUIRE(atSeven.size() == 1);
  REQUIRE(atEight.size() == 1);
  CHECK(atSeven[0].second == mine);
  CHECK(atEight[0].second == theirs);
}

TEST_CASE("an execution reaches both owners once each, and one owner once") {
  Wired wired;
  const int seven = wired.loginAs(7, 42);
  const int eight = wired.loginAs(8, 43);
  std::vector<char> mine = acceptedEvent(10, 7, 900);
  std::vector<char> theirs = acceptedEvent(20, 8, 800);
  std::vector<char> alsoMine = acceptedEvent(11, 7, 901);
  wired.gateway.onEvent(mine.data(), mine.size());
  wired.gateway.onEvent(theirs.data(), theirs.size());
  wired.gateway.onEvent(alsoMine.data(), alsoMine.size());
  wired.read(seven);
  wired.read(eight);

  std::vector<char> crossed = executedEvent(10, 20);
  wired.gateway.onEvent(crossed.data(), crossed.size());
  CHECK(wired.read(seven).size() == 1);
  CHECK(wired.read(eight).size() == 1);

  std::vector<char> selfCrossed = executedEvent(10, 11);
  wired.gateway.onEvent(selfCrossed.data(), selfCrossed.size());
  CHECK(wired.read(seven).size() == 1);
  CHECK(wired.read(eight).empty());
}

TEST_CASE("what nobody owns is counted rather than guessed at") {
  Wired wired;
  wired.loginAs(7, 42);
  std::vector<char> stranger = executedEvent(500, 501);
  wired.gateway.onEvent(stranger.data(), stranger.size());
  CHECK(wired.gateway.unroutable() == 2);
}
