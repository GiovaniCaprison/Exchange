// The order entry client, proved against the real chain: a real gateway, a real sequencer, a
// real matcher, and the client's bytes shuttled the way a socket would. A session opens and
// orders live; executions sign positions on both sides; a mid-session reconnect resumes the
// stream with nothing delivered twice and nothing lost; a fresh client rebuilds its resting book
// from sequence zero; and the gate's refusal dies on the session plane, leaving the numbered
// stream untouched.

#include "orderentry.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <tuple>
#include <vector>

#include "clock.hpp"
#include "flow.hpp"
#include "gateway.hpp"
#include "harness.hpp"
#include "partition.hpp"
#include "submission.hpp"

using namespace exchange::ecosystem;
namespace sbe = exchange::protocol;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;
using exchange::sequencer::test::submissionRecord;

namespace {

using Gate = exchange::risk::Risk<VirtualClock>;
using Machine = exchange::gateway::Gateway<CapturingLink, VirtualClock, Gate>;
using Client = OrderEntry<VirtualClock>;

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-orderentry-" + name);
}

// The whole venue plus one gateway, with byte shuttles where the sockets would be. Gateway zero
// is the reference-data carrier the tests drive directly; gateway one is the real machine.
struct Rig {
  VirtualClock clock;
  CapturingLink submissions;
  Gate risk;
  Machine gateway;
  exchange::sequencer::test::Wired venue;
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  CommandWriter writer;
  std::uint64_t referenceSequence = 0;
  std::size_t submissionsConsumed = 0;
  std::size_t venueConsumed = 0;
  std::size_t ackConsumed = 0;
  std::size_t eventConsumed = 0;

  explicit Rig(const std::string& journalPath,
               const exchange::risk::Limits eight = exchange::risk::Limits{})
      : risk(clock, {{7, {}}, {8, eight}}),
        gateway(submissions, clock, risk, 1, {{7, 42}, {8, 43}}),
        venue(journalPath, 2) {}

  // Reference data and session state ride gateway zero, the venue operator's carrier here.
  void reference(const CommandWriter::Framed& command) {
    std::vector<char> record = submissionRecord(0, ++referenceSequence, command.bytes);
    venue.sequencer.onSubmission(record.data(), record.size());
    pump();
  }

  // One beat of the whole room, repeated until nothing moves: clients into the gateway, the
  // gateway into the sequencer, the sequenced stream into the matcher, and everything flowing
  // back out to whoever is connected.
  void pump(Client* one = nullptr, const int slotOne = -1, Client* two = nullptr,
            const int slotTwo = -1) {
    bool moved = true;
    while (moved) {
      moved = false;
      moved |= shuttle(one, slotOne);
      moved |= shuttle(two, slotTwo);
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
        std::vector<char> one_(
            acks.begin() + static_cast<long>(ackConsumed),
            acks.begin() + static_cast<long>(ackConsumed + exchange::sequencer::ACK_BYTES));
        gateway.onAck(one_.data(), one_.size());
        ackConsumed += exchange::sequencer::ACK_BYTES;
        moved = true;
      }
      const std::vector<char>& seen = events.captured();
      while (eventConsumed < seen.size()) {
        sbe::MessageHeader wrap;
        std::vector<char> copy(seen.begin() + static_cast<long>(eventConsumed), seen.end());
        wrap.wrap(copy.data(), 0, 0, copy.size());
        const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
        gateway.onEvent(copy.data(), length);
        eventConsumed += length;
        moved = true;
      }
    }
  }

  bool shuttle(Client* client, const int slot) {
    if (client == nullptr || slot < 0) {
      return false;
    }
    bool moved = false;
    const auto [toVenue, toVenueSize] = client->outbound();
    if (toVenueSize > 0) {
      gateway.received(slot, toVenue, toVenueSize);
      client->drainedBy(toVenueSize);
      moved = true;
    }
    const auto [toClient, toClientSize] = gateway.outbound(slot);
    if (toClientSize > 0) {
      client->received(toClient, toClientSize);
      gateway.drained(slot, toClientSize);
      moved = true;
    }
    return moved;
  }

  int connect(Client& client) {
    const int slot = gateway.opened();
    client.connected();
    pump(&client, slot);
    return slot;
  }

  void open() {
    reference(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
    reference(writer.session(1, sbe::SessionState::CONTINUOUS));
  }
};

std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t, std::uint8_t>> liveBook(
    const Client& client) {
  std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t, std::uint8_t>> rows;
  client.forEachLive([&](const Client::Order& order) {
    rows.emplace_back(order.orderId, order.price, order.remaining, order.side);
  });
  std::sort(rows.begin(), rows.end());
  return rows;
}

}  // namespace

TEST_CASE("a session opens, orders live, and executions sign both positions") {
  const std::filesystem::path journal = scratch("trades.exj");
  Rig rig(journal.string());
  rig.open();
  Client seven(rig.clock, 7, 42);
  Client eight(rig.clock, 8, 43);
  const int slotSeven = rig.connect(seven);
  const int slotEight = rig.connect(eight);
  REQUIRE(seven.established());
  REQUIRE(eight.established());

  const std::uint64_t bid = seven.newOrder(1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                                           sbe::TimeInForce::GOOD_TILL_CANCEL, 100'000, 10);
  rig.pump(&seven, slotSeven, &eight, slotEight);
  CHECK(seven.liveOf(bid));
  CHECK(seven.remainingOf(bid) == 10);

  eight.newOrder(1, sbe::Side::SELL, sbe::Pricing::LIMIT, sbe::TimeInForce::IMMEDIATE_OR_CANCEL,
                 100'000, 10);
  rig.pump(&seven, slotSeven, &eight, slotEight);

  CHECK(seven.positionOf(1) == 10);
  CHECK(eight.positionOf(1) == -10);
  CHECK(!seven.liveOf(bid));
  CHECK(seven.executions() == 1);
  CHECK(eight.executions() == 1);
  std::filesystem::remove(journal);
}

TEST_CASE("a reconnection resumes the stream with nothing twice and nothing lost") {
  const std::filesystem::path journal = scratch("reconnect.exj");
  Rig rig(journal.string());
  rig.open();
  Client seven(rig.clock, 7, 42);
  Client eight(rig.clock, 8, 43);
  int slotSeven = rig.connect(seven);
  const int slotEight = rig.connect(eight);

  const std::uint64_t bid = seven.newOrder(1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                                           sbe::TimeInForce::GOOD_TILL_CANCEL, 100'000, 10);
  rig.pump(&seven, slotSeven, &eight, slotEight);
  REQUIRE(seven.liveOf(bid));
  const std::uint64_t heard = seven.seen();

  // The transport dies; the venue does not. The counterparty trades against the resting bid
  // while its owner is gone, and the gateway holds the story for the reconnection.
  rig.gateway.closed(slotSeven);
  seven.disconnected();
  eight.newOrder(1, sbe::Side::SELL, sbe::Pricing::LIMIT, sbe::TimeInForce::IMMEDIATE_OR_CANCEL,
                 100'000, 6);
  rig.pump(nullptr, -1, &eight, slotEight);
  CHECK(seven.positionOf(1) == 0);

  slotSeven = rig.connect(seven);
  rig.pump(&seven, slotSeven, &eight, slotEight);
  REQUIRE(seven.established());
  CHECK(seven.positionOf(1) == 6);
  CHECK(seven.remainingOf(bid) == 4);
  CHECK(seven.liveOf(bid));
  CHECK(seven.seen() > heard);
  std::filesystem::remove(journal);
}

TEST_CASE("a fresh client rebuilds its resting book from sequence zero") {
  const std::filesystem::path journal = scratch("rebuild.exj");
  Rig rig(journal.string());
  rig.open();
  Client original(rig.clock, 7, 42);
  const int slot = rig.connect(original);
  original.newOrder(1, sbe::Side::BUY, sbe::Pricing::LIMIT, sbe::TimeInForce::GOOD_TILL_CANCEL,
                    100'000, 10);
  original.newOrder(1, sbe::Side::BUY, sbe::Pricing::LIMIT, sbe::TimeInForce::GOOD_TILL_CANCEL,
                    99'995, 7);
  original.newOrder(1, sbe::Side::SELL, sbe::Pricing::LIMIT, sbe::TimeInForce::GOOD_TILL_CANCEL,
                    100'010, 5);
  rig.pump(&original, slot);
  REQUIRE(original.liveOrders() == 3);

  rig.gateway.closed(slot);
  original.disconnected();

  // Amnesia: a new client, the same participant, nothing remembered, everything replayed.
  Client fresh(rig.clock, 7, 42);
  const int again = rig.connect(fresh);
  rig.pump(&fresh, again);
  REQUIRE(fresh.established());
  CHECK(fresh.seen() == original.seen());
  CHECK(liveBook(fresh) == liveBook(original));
  std::filesystem::remove(journal);
}

TEST_CASE("the gate's refusal dies on the session plane, and the numbered stream never blinks") {
  const std::filesystem::path journal = scratch("refused.exj");
  exchange::risk::Limits tight;
  tight.credit = 1'000;
  Rig rig(journal.string(), tight);
  rig.open();
  Client eight(rig.clock, 8, 43);
  const int slot = rig.connect(eight);
  REQUIRE(eight.established());
  const std::uint64_t before = eight.seen();

  // Far more notional than the gate will hold: refused at the door, never sequenced.
  const std::uint64_t hopeless = eight.newOrder(1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                                                sbe::TimeInForce::GOOD_TILL_CANCEL, 100'000, 10);
  rig.pump(&eight, slot);
  CHECK(eight.refusals() == 1);
  CHECK(!eight.liveOf(hopeless));
  CHECK(eight.liveOrders() == 0);
  CHECK(eight.seen() == before);
  std::filesystem::remove(journal);
}

TEST_CASE("the pulse keeps a quiet session alive from the client's side") {
  const std::filesystem::path journal = scratch("pulse.exj");
  Rig rig(journal.string());
  rig.open();
  Client seven(rig.clock, 7, 42);
  const int slot = rig.connect(seven);
  REQUIRE(seven.established());

  // Twenty quiet seconds, heartbeats carrying both sides past every deadline.
  for (int beat = 0; beat < 20; beat++) {
    rig.clock.advance(1'000'000'000ULL);
    seven.onTick();
    rig.gateway.onTick();
    rig.pump(&seven, slot);
  }
  CHECK(seven.established());
  CHECK(!seven.starving());
  CHECK(!rig.gateway.wantsClose(slot));
  std::filesystem::remove(journal);
}
