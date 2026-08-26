// The event contexts: the partition's event stream numbers gap free from 1, every event names the
// input sequence it answers, the timestamp is the answered command's own, carried unchanged (P-3),
// and the ring sees one publish per command, which is the per-command batch a downstream consumer
// frames on.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <vector>

#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::matcher;
using namespace exchange::matcher::test;

TEST_CASE("every event carries its attribution and the stream numbers gap free") {
  CapturingRing ring;
  Partition<CapturingRing> partition(ring);
  CommandWriter writer;

  std::vector<CommandWriter::Framed> commands;
  commands.push_back(writer.instrument(1, 5, 1, 5, 1'000'000, 500, 100'000, false));
  commands.push_back(writer.session(1, sbe::SessionState::CONTINUOUS));
  commands.push_back(writer.newOrder(1, 11, 1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                                     sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 50, 0, 0,
                                     0, 0));
  commands.push_back(writer.newOrder(1, 12, 2, sbe::Side::SELL, sbe::Pricing::LIMIT,
                                     sbe::TimeInForce::GOOD_TILL_CANCEL, false, 99'995, 30, 0, 0, 0,
                                     0));
  commands.push_back(writer.cancel(1, 11, 1));
  for (auto& framed : commands) {
    partition.onCommand(framed.bytes.data(), 0, framed.bytes.size());
  }

  const std::vector<EventView> events = readEvents(ring.captured());
  REQUIRE(events.size() >= 6);
  std::uint64_t expectedSequence = 0;
  for (const EventView& event : events) {
    // Gap free from 1, whatever the event and whichever book it describes.
    CHECK(event.sequence == ++expectedSequence);
    // The timestamp is a pure function of the input sequence here, so carrying it unchanged is
    // checkable: the sequencer stamped 1'000'000'000'000 + sequence.
    CHECK(event.timestamp == 1'000'000'000'000ULL + event.inputSequence);
    CHECK(event.instrumentId == 1);
  }

  // The state change answers command 2, the buy's accepted and rested answer command 3, the
  // sell's accepted and the execution answer command 4, and the removal answers the cancel at 5.
  CHECK(events[0].inputSequence == 2);
  CHECK(events[1].inputSequence == 3);
  CHECK(events[2].inputSequence == 3);
  CHECK(events[3].inputSequence == 4);
  CHECK(events[4].inputSequence == 4);
  CHECK(events[5].inputSequence == 5);

  // One publish per command: the batch boundary a consumer frames on.
  CHECK(ring.publishes() == commands.size());
}
