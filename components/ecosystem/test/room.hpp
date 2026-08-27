// The room: a whole venue in one process with participants actually seated at it. The real
// gateway, the real sequencer, the real matcher, the real market data builder and publisher, and
// N clients each holding an order entry session and its own feed handler, with bytes shuttled
// where sockets would be. Time is one virtual clock; every actor is a pure function of the
// stream and its seeds, so a day driven twice through the room is the same day, journals and
// all, which is the claim the bots suite holds.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "builder.hpp"
#include "clock.hpp"
#include "feedhandler.hpp"
#include "feedtest.hpp"
#include "gateway.hpp"
#include "harness.hpp"
#include "orderentry.hpp"
#include "partition.hpp"
#include "publisher.hpp"
#include "submission.hpp"

namespace exchange::ecosystem::test {

namespace sbe = ::exchange::protocol;
using exchange::marketdata::test::CapturingSink;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;
using exchange::sequencer::test::RewindChannel;
using exchange::sequencer::test::submissionRecord;

using Gate = exchange::risk::Risk<VirtualClock>;
using Machine = exchange::gateway::Gateway<CapturingLink, VirtualClock, Gate>;
using Entry = OrderEntry<VirtualClock>;
using Feed = FeedHandler<RewindChannel>;

// One participant: a session, a feed handler, and the chair they sit in.
struct Seat {
  RewindChannel channel;
  Feed feed;
  Entry entry;
  int slot = -1;
  std::size_t packetsConsumed = 0;

  Seat(VirtualClock& clock, const std::uint32_t participantId, const std::uint64_t secret)
      : feed(channel), entry(clock, participantId, secret) {}
};

struct Room {
  VirtualClock clock;
  CapturingLink submissions;
  Gate risk;
  Machine gateway;
  exchange::sequencer::test::Wired venue;
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  CapturingSink a;
  CapturingSink b;
  exchange::marketdata::Publisher<CapturingSink, CapturingSink> publisher{a, b};
  exchange::marketdata::Builder<exchange::marketdata::Publisher<CapturingSink, CapturingSink>>
      builder{publisher};
  CommandWriter writer;
  std::vector<std::unique_ptr<Seat>> seats;
  std::uint64_t referenceSequence = 0;
  std::size_t submissionsConsumed = 0;
  std::size_t venueConsumed = 0;
  std::size_t ackConsumed = 0;
  std::size_t gatewayEventsConsumed = 0;
  std::size_t feedEventsConsumed = 0;

  explicit Room(const std::string& journalPath)
      : risk(clock, {{7, {}}, {8, {}}, {9, {}}, {10, {}}}),
        gateway(submissions, clock, risk, 1, {{7, 42}, {8, 43}, {9, 44}, {10, 45}}),
        venue(journalPath, 2) {}

  Seat& seat(const std::uint32_t participantId, const std::uint64_t secret) {
    seats.push_back(std::make_unique<Seat>(clock, participantId, secret));
    Seat& sat = *seats.back();
    sat.slot = gateway.opened();
    sat.entry.connected();
    pump();
    return sat;
  }

  // Reference data and session state ride gateway zero, the venue operator's carrier here.
  void reference(const CommandWriter::Framed& command) {
    std::vector<char> record = submissionRecord(0, ++referenceSequence, command.bytes);
    venue.sequencer.onSubmission(record.data(), record.size());
    pump();
  }

  // One beat of the whole room, repeated until nothing moves anywhere.
  void pump() {
    bool moved = true;
    while (moved) {
      moved = false;
      for (const std::unique_ptr<Seat>& seat : seats) {
        moved |= shuttle(*seat);
      }
      for (; submissionsConsumed < submissions.ranges.size(); submissionsConsumed++) {
        std::vector<char>& record = submissions.ranges[submissionsConsumed];
        venue.sequencer.onSubmission(record.data(), record.size());
        moved = true;
      }
      const std::vector<char>& sequenced = venue.out.captured();
      while (venueConsumed < sequenced.size()) {
        sbe::MessageHeader wrap;
        std::vector<char> copy(sequenced.begin() + static_cast<long>(venueConsumed),
                               sequenced.end());
        wrap.wrap(copy.data(), 0, 0, copy.size());
        const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
        partition.onCommand(copy.data(), 0, length);
        venueConsumed += length;
        moved = true;
      }
      const std::vector<char>& acks = venue.acks[1].captured();
      while (ackConsumed + exchange::sequencer::ACK_BYTES <= acks.size()) {
        std::vector<char> one(
            acks.begin() + static_cast<long>(ackConsumed),
            acks.begin() + static_cast<long>(ackConsumed + exchange::sequencer::ACK_BYTES));
        gateway.onAck(one.data(), one.size());
        ackConsumed += exchange::sequencer::ACK_BYTES;
        moved = true;
      }
      // The event stream fans out: the gateway routes owned events to sessions, and the market
      // data builder derives the public feed, each on its own cursor as the broadcast ring
      // seats them in deployment.
      moved |= drainEvents(gatewayEventsConsumed, [this](char* message, const std::size_t length) {
        gateway.onEvent(message, length);
      });
      const bool published = drainEvents(
          feedEventsConsumed,
          [this](char* message, const std::size_t length) { builder.onEvent(message, length); });
      if (published) {
        publisher.flush();
        moved = true;
      }
      for (const std::unique_ptr<Seat>& seat : seats) {
        for (; seat->packetsConsumed < a.packets.size(); seat->packetsConsumed++) {
          std::vector<char> copy = a.packets[seat->packetsConsumed];
          seat->feed.onPacket(copy.data(), copy.size());
          moved = true;
        }
      }
    }
  }

  template <typename Handler>
  bool drainEvents(std::size_t& cursor, Handler&& handler) {
    const std::vector<char>& seen = events.captured();
    bool moved = false;
    while (cursor < seen.size()) {
      sbe::MessageHeader wrap;
      std::vector<char> copy(seen.begin() + static_cast<long>(cursor), seen.end());
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      handler(copy.data(), length);
      cursor += length;
      moved = true;
    }
    return moved;
  }

  bool shuttle(Seat& seat) {
    if (seat.slot < 0) {
      return false;
    }
    bool moved = false;
    const auto [toVenue, toVenueSize] = seat.entry.outbound();
    if (toVenueSize > 0) {
      gateway.received(seat.slot, toVenue, toVenueSize);
      seat.entry.drainedBy(toVenueSize);
      moved = true;
    }
    const auto [toClient, toClientSize] = gateway.outbound(seat.slot);
    if (toClientSize > 0) {
      seat.entry.received(toClient, toClientSize);
      gateway.drained(seat.slot, toClientSize);
      moved = true;
    }
    return moved;
  }

  void open(const std::uint32_t instrumentId) {
    reference(writer.instrument(instrumentId, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
    reference(writer.session(instrumentId, sbe::SessionState::CONTINUOUS));
  }

  // One tick of simulated time: the clock moves, every session pulses, and the room settles.
  template <typename Actors>
  void tick(const std::uint64_t nanos, Actors&& actors) {
    clock.advance(nanos);
    actors();
    for (const std::unique_ptr<Seat>& seat : seats) {
      seat->entry.onTick();
    }
    gateway.onTick();
    pump();
  }
};

}  // namespace exchange::ecosystem::test
