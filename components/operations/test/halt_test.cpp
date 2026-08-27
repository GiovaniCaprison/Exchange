// The halt machine's law: prints inside the band change nothing; the print that breaches it,
// judged against the reference that stood before it, halts its instrument and only its
// instrument; a halted instrument reopens through an auction on the configured clock; the
// operator's hand works the same machinery; and the calendar outranks it all.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "clock.hpp"
#include "scheduler.hpp"
#include "submission.hpp"

using namespace exchange::operations;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;
namespace sbe = exchange::protocol;

namespace {

std::vector<char> printAt(const std::uint32_t instrument, const std::uint64_t streamTime,
                          const std::int64_t price) {
  char space[128] = {};
  sbe::OrderExecuted event;
  event.wrapAndApplyHeader(space, 0, sizeof space);
  event.context().sequence(1).inputSequence(1).timestamp(streamTime).instrumentId(instrument);
  event.context().reserved(0);
  event.executionId(1).aggressorOrderId(900).restingOrderId(901).price(price).quantity(1);
  return std::vector<char>(
      space, space + sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
}

std::vector<char> stateAt(const std::uint32_t instrument, const sbe::SessionState::Value state) {
  char space[128] = {};
  sbe::SessionStateChanged event;
  event.wrapAndApplyHeader(space, 0, sizeof space);
  event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(instrument).reserved(0);
  event.state(state);
  return std::vector<char>(space, space + sbe::MessageHeader::encodedLength() +
                                      sbe::SessionStateChanged::sbeBlockLength());
}

sbe::SessionState::Value stateOfRecord(std::vector<char>& record) {
  char* command = record.data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, record.size());
  sbe::SessionControl control;
  control.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        record.size());
  return control.state();
}

std::uint32_t instrumentOfRecord(std::vector<char>& record) {
  char* command = record.data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, record.size());
  sbe::SessionControl control;
  control.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        record.size());
  return control.context().instrumentId();
}

}  // namespace

TEST_CASE("the breaching print halts its instrument, which reopens through an auction") {
  VirtualClock wall;
  CapturingLink out;
  Config config;
  config.instruments = {1, 2};
  config.bandBasisPoints = 500;
  config.haltNanos = 1'000;
  config.auctionNanos = 500;
  Scheduler<CapturingLink, VirtualClock> scheduler(out, wall, config);

  std::vector<char> trading1 = stateAt(1, sbe::SessionState::CONTINUOUS);
  std::vector<char> trading2 = stateAt(2, sbe::SessionState::CONTINUOUS);
  scheduler.onEvent(trading1.data(), trading1.size());
  scheduler.onEvent(trading2.data(), trading2.size());

  // Ten calm prints around 10000 build the reference; five percent is the law.
  for (std::uint64_t at = 1; at <= 10; at++) {
    std::vector<char> calm = printAt(1, at, 10'000 + static_cast<std::int64_t>(at % 3));
    scheduler.onEvent(calm.data(), calm.size());
  }
  std::vector<char> nearEdge = printAt(1, 11, 10'400);
  scheduler.onEvent(nearEdge.data(), nearEdge.size());
  CHECK(scheduler.halts() == 0);

  // The wild print breaches; the halt is submitted for instrument one alone.
  std::vector<char> wild = printAt(1, 12, 11'000);
  scheduler.onEvent(wild.data(), wild.size());
  CHECK(scheduler.halts() == 1);
  REQUIRE(out.ranges.size() == 1);
  CHECK(stateOfRecord(out.ranges[0]) == sbe::SessionState::HALTED);
  CHECK(instrumentOfRecord(out.ranges[0]) == 1);

  // Further panic while halted changes nothing; instrument two trades on.
  std::vector<char> panic = printAt(1, 13, 20'000);
  scheduler.onEvent(panic.data(), panic.size());
  std::vector<char> elsewhere = printAt(2, 13, 10'000);
  scheduler.onEvent(elsewhere.data(), elsewhere.size());
  CHECK(scheduler.halts() == 1);

  // The pause passes, the reopening call opens, and continuous trading returns.
  wall.advance(1'100);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 2);
  CHECK(stateOfRecord(out.ranges[1]) == sbe::SessionState::OPENING_AUCTION);
  wall.advance(600);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 3);
  CHECK(stateOfRecord(out.ranges[2]) == sbe::SessionState::CONTINUOUS);
}

TEST_CASE("the operator's hand and the calendar's rank") {
  VirtualClock wall;
  CapturingLink out;
  Config config;
  config.instruments = {1};
  config.haltNanos = 1'000'000;
  config.calendar = {{wall.now() + 5'000, sbe::SessionState::CLOSED}};
  Scheduler<CapturingLink, VirtualClock> scheduler(out, wall, config);
  std::vector<char> trading = stateAt(1, sbe::SessionState::CONTINUOUS);
  scheduler.onEvent(trading.data(), trading.size());

  scheduler.haltNow(1);
  CHECK(scheduler.halts() == 1);
  REQUIRE(out.ranges.size() == 1);
  CHECK(stateOfRecord(out.ranges[0]) == sbe::SessionState::HALTED);
  // Halting a halted instrument is a refusal to double-fire.
  scheduler.haltNow(1);
  CHECK(scheduler.halts() == 1);

  // The close arrives mid-halt and outranks the reopening machinery entirely.
  wall.advance(6'000);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 2);
  CHECK(stateOfRecord(out.ranges[1]) == sbe::SessionState::CLOSED);
  wall.advance(10'000'000);
  scheduler.onTick();
  CHECK(out.ranges.size() == 2);
}
