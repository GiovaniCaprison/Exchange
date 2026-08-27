// The calendar's law: transitions fire when their time comes and not before, once each, in
// order, to every instrument; and a scheduler that hears no acknowledgments resubmits, leaning
// on the sequencer's dedupe like every other submitter.

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

sbe::SessionState::Value stateOfRecord(std::vector<char>& record) {
  char* command = record.data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, record.size());
  sbe::SessionControl control;
  control.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        record.size());
  return control.state();
}

}  // namespace

TEST_CASE("the calendar fires on time, once, in order, for every instrument") {
  VirtualClock wall;
  CapturingLink out;
  Config config;
  config.instruments = {1, 2};
  const std::uint64_t start = wall.now();
  config.calendar = {{start + 100, sbe::SessionState::PRE_OPEN},
                     {start + 200, sbe::SessionState::OPENING_AUCTION},
                     {start + 300, sbe::SessionState::CONTINUOUS}};
  Scheduler<CapturingLink, VirtualClock> scheduler(out, wall, config);

  scheduler.onTick();
  CHECK(out.ranges.empty());

  wall.advance(150);
  scheduler.onTick();
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 2);
  CHECK(stateOfRecord(out.ranges[0]) == sbe::SessionState::PRE_OPEN);
  CHECK(stateOfRecord(out.ranges[1]) == sbe::SessionState::PRE_OPEN);

  // A late tick catches up the whole backlog, still in calendar order.
  wall.advance(200);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 6);
  CHECK(stateOfRecord(out.ranges[2]) == sbe::SessionState::OPENING_AUCTION);
  CHECK(stateOfRecord(out.ranges[4]) == sbe::SessionState::CONTINUOUS);
}

TEST_CASE("silence from the sequencer means resubmission, never loss") {
  VirtualClock wall;
  CapturingLink out;
  Config config;
  config.instruments = {1};
  config.calendar = {{wall.now() + 10, sbe::SessionState::CONTINUOUS}};
  config.resubmitAfterNanos = 1'000;
  Scheduler<CapturingLink, VirtualClock> scheduler(out, wall, config);

  wall.advance(20);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 1);
  CHECK(scheduler.submitted() == 1);

  // No acknowledgment arrives; the allowance passes; the same bytes go again.
  wall.advance(2'000);
  scheduler.onTick();
  CHECK(scheduler.resubmitted() == 1);
  REQUIRE(out.ranges.size() == 2);
  CHECK(out.ranges[0] == out.ranges[1]);
}

TEST_CASE("reference data rides the same acknowledged carrier") {
  VirtualClock wall;
  CapturingLink out;
  Config config;
  config.instruments = {1};
  Scheduler<CapturingLink, VirtualClock> scheduler(out, wall, config);

  Scheduler<CapturingLink, VirtualClock>::Definition definition;
  definition.instrumentId = 1;
  definition.tickSize = 5;
  definition.lotSize = 1;
  definition.minPrice = 5;
  definition.maxPrice = 1'000'000;
  definition.bandWidth = 100'000'000;
  definition.openingReference = 100'000;
  scheduler.define(definition);

  REQUIRE(out.ranges.size() == 1);
  char* command = out.ranges[0].data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, out.ranges[0].size());
  REQUIRE(wrap.templateId() == sbe::InstrumentDefinition::sbeTemplateId());
  sbe::InstrumentDefinition decoded;
  decoded.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        out.ranges[0].size());
  CHECK(decoded.context().instrumentId() == 1);
  CHECK(decoded.tickSize() == 5);
  CHECK(decoded.openingReference() == 100'000);

  // Unacknowledged reference data resubmits as identical bytes, like everything the carrier owes.
  wall.advance(600'000'000);
  scheduler.onTick();
  REQUIRE(out.ranges.size() == 2);
  CHECK(out.ranges[1] == out.ranges[0]);
}
