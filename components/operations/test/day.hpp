// The scripted-day machinery: a whole venue in one process, the scheduler wired beside the
// client gateway into a real sequencer feeding a real matcher, the events flowing back to the
// scheduler, and every clock in the room owned by the script. A day driven twice through this
// must journal identical bytes, halts included, which is the component's flagship claim.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "clock.hpp"
#include "flow.hpp"
#include "harness.hpp"
#include "partition.hpp"
#include "scheduler.hpp"
#include "submission.hpp"

namespace exchange::operations::test {

namespace sbe = ::exchange::protocol;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::test::CapturingLink;
using exchange::sequencer::test::submissionRecord;

struct Day {
  exchange::sequencer::VirtualClock wall;
  CapturingLink operationsOut;
  Scheduler<CapturingLink, exchange::sequencer::VirtualClock> scheduler;
  exchange::sequencer::test::Wired venue;
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  std::uint64_t clientSequence = 0;
  std::size_t venueConsumed = 0;
  std::size_t operationsConsumed = 0;
  std::size_t ackConsumed = 0;
  std::size_t eventConsumed = 0;

  Day(const std::string& journalPath, Config config)
      : scheduler(operationsOut, wall, std::move(config)), venue(journalPath, 2) {}

  // One beat: the scheduler acts on the clock, everything submitted reaches the sequencer, the
  // sequenced commands reach the matcher, and the events and acknowledgments flow back.
  void pump() {
    scheduler.onTick();
    for (; operationsConsumed < operationsOut.ranges.size(); operationsConsumed++) {
      std::vector<char>& record = operationsOut.ranges[operationsConsumed];
      venue.sequencer.onSubmission(record.data(), record.size());
    }
    const std::vector<char>& sequenced = venue.out.captured();
    while (venueConsumed < sequenced.size()) {
      sbe::MessageHeader wrap;
      std::vector<char> copy(sequenced.begin() + static_cast<long>(venueConsumed), sequenced.end());
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      partition.onCommand(copy.data(), 0, length);
      venueConsumed += length;
    }
    const std::vector<char>& acks = venue.acks[1].captured();
    while (ackConsumed + exchange::sequencer::ACK_BYTES <= acks.size()) {
      std::vector<char> one(
          acks.begin() + static_cast<long>(ackConsumed),
          acks.begin() + static_cast<long>(ackConsumed + exchange::sequencer::ACK_BYTES));
      scheduler.onAck(one.data(), one.size());
      ackConsumed += exchange::sequencer::ACK_BYTES;
    }
    const std::vector<char>& seen = events.captured();
    while (eventConsumed < seen.size()) {
      sbe::MessageHeader wrap;
      std::vector<char> copy(seen.begin() + static_cast<long>(eventConsumed), seen.end());
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      scheduler.onEvent(copy.data(), length);
      eventConsumed += length;
    }
  }

  void client(const CommandWriter::Framed& command) {
    std::vector<char> record = submissionRecord(0, ++clientSequence, command.bytes);
    venue.sequencer.onSubmission(record.data(), record.size());
    pump();
  }

  void advance(const std::uint64_t nanos) {
    wall.advance(nanos);
    pump();
  }
};

}  // namespace exchange::operations::test
