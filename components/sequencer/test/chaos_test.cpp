// The chaos suite: the sequencer killed at chosen points, the witness deciding who leads, and
// one property held every run: the published stream stitched across epochs is gap free,
// duplicate free, and contains every command any gateway was ever acknowledged; a matcher
// consuming the stitched stream emits bytes identical to one replaying the surviving journal,
// which is the availability story executed (P-2). Time is virtual throughout, so every expiry is
// scripted and nothing here can flake.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "clock.hpp"
#include "flow.hpp"
#include "journal.hpp"
#include "leadership.hpp"
#include "partition.hpp"
#include "ranges.hpp"
#include "rewinder.hpp"
#include "stream.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-chaos-" + name);
}

std::vector<char> stamped(const std::vector<CommandWriter::Framed>& flow) {
  std::vector<char> bytes;
  for (const CommandWriter::Framed& framed : flow) {
    bytes.insert(bytes.end(), framed.bytes.begin(), framed.bytes.end());
  }
  return bytes;
}

// A gateway as the chaos suite plays it: numbering its submissions, remembering what was never
// acknowledged, and resubmitting exactly that after a failover, in order.
struct TestGateway {
  std::uint32_t id;
  std::uint64_t next = 1;
  std::map<std::uint64_t, std::vector<char>> unacked;

  std::vector<char> submit(const std::vector<char>& command) {
    std::vector<char> record = submissionRecord(id, next, command);
    unacked[next] = record;
    next++;
    return record;
  }

  void covered(const std::vector<char>& ackBytes) {
    for (const Ack& ack : readAcks(ackBytes)) {
      unacked.erase(ack.gatewaySequence);
    }
  }
};

// The consuming side: a packet source over a rewind channel, its deliveries concatenated, its
// rewinds served from whichever journal survives.
struct Consumer {
  RewindChannel channel;
  common::stream::PacketSource<RewindChannel> source{1, 1, channel};
  std::vector<char> delivered;

  void feed(const std::vector<std::vector<char>>& packets, Rewinder& rewinder) {
    const auto handler = [&](char* bytes, const std::size_t length) {
      delivered.insert(delivered.end(), bytes, bytes + length);
    };
    for (const std::vector<char>& packet : packets) {
      std::vector<char> copy = packet;
      source.onPacket(copy.data(), copy.size(), handler);
      while (!channel.requests.empty()) {
        const RewindChannel::Request request = channel.requests.front();
        channel.requests.erase(channel.requests.begin());
        rewinder.serve(request.firstSequence, request.count,
                       [&](char* bytes, const std::size_t length) {
                         source.onPacket(bytes, length, handler);
                       });
      }
    }
  }
};

// The stitched stream's law: sequences contiguous from one, and a matcher consuming it emits the
// bytes a matcher replaying the surviving journal emits.
void holdTheProperty(const std::vector<char>& stitched, const std::string& journalPath) {
  std::uint64_t expected = 1;
  std::size_t at = 0;
  while (at < stitched.size()) {
    namespace sbe = exchange::protocol;
    sbe::MessageHeader wrap;
    std::vector<char> copy(stitched.begin() + static_cast<long>(at), stitched.end());
    wrap.wrap(copy.data(), 0, 0, copy.size());
    std::uint64_t sequence = 0;
    std::memcpy(&sequence, copy.data() + sbe::MessageHeader::encodedLength(), sizeof sequence);
    REQUIRE(sequence == expected);
    expected++;
    at += sbe::MessageHeader::encodedLength() + wrap.blockLength();
  }

  exchange::matcher::test::CapturingRing consumedRing;
  exchange::matcher::Partition<exchange::matcher::test::CapturingRing> consumed(consumedRing);
  std::vector<char> live = stitched;
  std::size_t cursor = 0;
  while (cursor < live.size()) {
    namespace sbe = exchange::protocol;
    sbe::MessageHeader wrap;
    wrap.wrap(live.data(), cursor, 0, live.size());
    const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
    consumed.onCommand(live.data(), cursor, length);
    cursor += length;
  }

  exchange::matcher::test::CapturingRing replayedRing;
  exchange::matcher::Partition<exchange::matcher::test::CapturingRing> replayed(replayedRing);
  common::journal::Read log = common::journal::read(journalPath);
  for (std::size_t entry = 0; entry < log.count(); entry++) {
    replayed.onCommand(log.messages.data() + log.offsets[entry], 0, log.lengths[entry]);
  }
  CHECK(consumedRing.captured() == replayedRing.captured());
  CHECK(!consumedRing.captured().empty());
}

}  // namespace

TEST_CASE("the witness's law") {
  VirtualClock clock;
  Witness<VirtualClock> witness(clock, 100);

  // The first claimant takes epoch one, and renewal keeps it.
  CHECK(witness.onRequest(1, 1, 0).granted);
  clock.advance(60);
  CHECK(witness.onRequest(1, 1, 0).granted);
  clock.advance(60);

  // A live lease cannot be usurped, whatever epoch is asked for.
  const auto refused = witness.onRequest(2, 2, 0);
  CHECK(!refused.granted);
  CHECK(refused.holder == 1);
  CHECK(refused.epoch == 1);

  // An expired one can, and only forward: the old epoch is dead everywhere after.
  clock.advance(200);
  CHECK(witness.onRequest(2, 2, 0).granted);
  CHECK(witness.holder() == 2);
  CHECK(!witness.onRequest(1, 1, 0).granted);
}

TEST_CASE("a partitioned primary cannot be usurped while alive, and is ignored once expired") {
  VirtualClock clock;
  Witness<VirtualClock> witness(clock, 1000);
  Lease<VirtualClock> primary(clock, 1);
  Lease<VirtualClock> standby(clock, 2);

  primary.onAnswer(witness.onRequest(1, 1, 0));
  CHECK(primary.held());

  // The partition begins: the primary can no longer reach the witness. Before expiry the
  // standby's bid fails; the primary still holds by its own clock and may publish.
  clock.advance(600);
  standby.onAnswer(witness.onRequest(2, 2, 0));
  CHECK(!standby.held());
  CHECK(primary.held());

  // Expiry: the primary's own clock tells it to stop, and only then can the standby win.
  clock.advance(500);
  CHECK(!primary.held());
  standby.onAnswer(witness.onRequest(2, 2, 0));
  CHECK(standby.held());
  CHECK(standby.epoch() == 2);

  // A consumer that adopted the new epoch drops the dead leader's straggler outright.
  RewindChannel channel;
  common::stream::PacketSource<RewindChannel> source(1, 2, channel);
  char buffer[128];
  common::ranges::Builder builder(buffer, sizeof buffer);
  builder.open(1, 1);
  const std::size_t length = builder.close();
  bool deliveredAnything = false;
  source.onPacket(buffer, length, [&](char*, std::size_t) { deliveredAnything = true; });
  CHECK(!deliveredAnything);
}

TEST_CASE(
    "killed after replication: the suffix publishes under the new epoch and retries are"
    " answered from inherited windows") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(101, 400);
  const std::filesystem::path primaryPath = scratch("after-rep.exj");
  const std::filesystem::path standbyPath = scratch("after-rep.standby.exj");
  std::vector<TestGateway> gateways{{0}, {1}};
  std::vector<std::vector<char>> epochOnePackets;
  std::vector<std::vector<char>> epochTwoPackets;
  Inherited inherited;
  std::vector<char> expected = stamped(flow);
  const std::uint64_t publishedByOld = 150;

  {
    Wired old(primaryPath.string(), 2, Durability::SAFE);
    CapturingRing standbyAcks;
    common::journal::Writer standbyJournal(standbyPath.string());
    Standby<CapturingRing, common::journal::Writer> standby(standbyJournal, standbyAcks, 2);

    for (std::size_t at = 0; at < flow.size(); at++) {
      std::vector<char> record = gateways[at % 2].submit(flow[at].bytes);
      old.sequencer.onSubmission(record.data(), record.size());
    }
    // Everything replicated; the acknowledgment covering 150 is the last one the primary lives
    // to process, so it publishes and acknowledges that far and no further.
    for (std::vector<char>& range : old.link.ranges) {
      standby.onRange(range.data(), range.size());
    }
    old.sequencer.onReplicationAck(publishedByOld);
    old.sequencer.heartbeat();
    standby.onRange(old.link.ranges.back().data(), old.link.ranges.back().size());
    CHECK(old.sequencer.published() == publishedByOld);
    CHECK(standby.held() == flow.size());
    CHECK(standby.publishedFloor() == publishedByOld + 1);
    for (std::size_t gateway = 0; gateway < 2; gateway++) {
      gateways[gateway].covered(old.acks[gateway].captured());
    }
    epochOnePackets = old.packets.packets;
    inherited = standby.inherited();
    // The primary dies here, mid-everything, taking its unprocessed acknowledgments with it.
  }

  // The witness hands epoch two to the standby, which becomes the primary on its own journal.
  VirtualClock clock;
  Witness<VirtualClock> witness(clock, 100);
  CHECK(witness.onRequest(1, 1, 0).granted);
  clock.advance(200);
  CHECK(witness.onRequest(2, 2, inherited.held).granted);

  CapturingRing out;
  std::vector<CapturingRing> acks(2);
  CapturingPacketSink packets;
  CapturingLink link;
  common::journal::Writer journal(standbyPath.string(), true);
  ScriptedClock stamps;
  Sequencer<CapturingRing, CapturingRing, CapturingPacketSink, common::journal::Writer,
            ScriptedClock, CapturingLink>
      leader(out, acks, packets, journal, stamps, link, Durability::LOCAL, 2);
  leader.inherit(inherited);

  // First act: publish the replicated suffix the old primary never did.
  {
    common::journal::Read held = common::journal::read(standbyPath.string());
    for (std::size_t at = 0; at < held.count(); at++) {
      std::uint64_t sequence = 0;
      std::memcpy(&sequence,
                  held.messages.data() + held.offsets[at] +
                      exchange::protocol::MessageHeader::encodedLength(),
                  sizeof sequence);
      if (sequence >= inherited.publishedFloor) {
        leader.republish(held.messages.data() + held.offsets[at], held.lengths[at]);
      }
    }
    leader.flush();
  }
  CHECK(leader.published() == flow.size());

  // Second act: the gateways resubmit everything unacknowledged, and every one of them is a
  // replicated command, so the inherited windows answer with the places they already hold.
  std::size_t resubmitted = 0;
  for (TestGateway& gateway : gateways) {
    for (auto& [gatewaySequence, record] : gateway.unacked) {
      std::vector<char> copy = record;
      leader.onSubmission(copy.data(), copy.size());
      resubmitted++;
    }
    gateway.covered(acks[gateway.id].captured());
  }
  CHECK(resubmitted > 0);
  CHECK(leader.duplicates() == resubmitted);
  CHECK(leader.sequence() == flow.size());
  for (const TestGateway& gateway : gateways) {
    CHECK(gateway.unacked.empty());
  }
  epochTwoPackets = packets.packets;

  // The stitched stream: epoch one's packets, then epoch two's; the overlap is skipped, the
  // result is the whole flow, and the matcher cannot tell there was a failover.
  Consumer consumer;
  Rewinder rewinder(standbyPath.string(), 2);
  consumer.feed(epochOnePackets, rewinder);
  consumer.feed(epochTwoPackets, rewinder);
  CHECK(consumer.delivered == expected);
  holdTheProperty(consumer.delivered, standbyPath.string());

  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("killed before replication: the lost tail is resubmitted and sequenced exactly once") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(103, 300);
  const std::filesystem::path primaryPath = scratch("before-rep.exj");
  const std::filesystem::path standbyPath = scratch("before-rep.standby.exj");
  std::vector<TestGateway> gateways{{0}, {1}};
  std::vector<std::vector<char>> epochOnePackets;
  Inherited inherited;
  const std::uint64_t replicated = 180;

  {
    Wired old(primaryPath.string(), 2, Durability::SAFE);
    CapturingRing standbyAcks;
    common::journal::Writer standbyJournal(standbyPath.string());
    Standby<CapturingRing, common::journal::Writer> standby(standbyJournal, standbyAcks, 2);
    for (std::size_t at = 0; at < flow.size(); at++) {
      std::vector<char> record = gateways[at % 2].submit(flow[at].bytes);
      old.sequencer.onSubmission(record.data(), record.size());
    }
    // Only the first 180 ranges ever reach the standby: the rest die with the link.
    for (std::uint64_t at = 0; at < replicated; at++) {
      standby.onRange(old.link.ranges[at].data(), old.link.ranges[at].size());
    }
    old.sequencer.onReplicationAck(replicated);
    old.sequencer.heartbeat();
    standby.onRange(old.link.ranges.back().data(), old.link.ranges.back().size());
    // The marker names 181, exactly the standby's held plus one, so it lands as the floor; the
    // replicated suffix to republish is therefore empty, which is the truth.
    CHECK(standby.held() == replicated);
    for (std::size_t gateway = 0; gateway < 2; gateway++) {
      gateways[gateway].covered(old.acks[gateway].captured());
    }
    epochOnePackets = old.packets.packets;
    inherited = standby.inherited();
  }
  CHECK(inherited.held == replicated);

  CapturingRing out;
  std::vector<CapturingRing> acks(2);
  CapturingPacketSink packets;
  CapturingLink link;
  common::journal::Writer journal(standbyPath.string(), true);
  ScriptedClock stamps;
  Sequencer<CapturingRing, CapturingRing, CapturingPacketSink, common::journal::Writer,
            ScriptedClock, CapturingLink>
      leader(out, acks, packets, journal, stamps, link, Durability::LOCAL, 2);
  leader.inherit(inherited);

  // Everything unacknowledged comes back; what was replicated is re-answered, and what was lost
  // is sequenced fresh, once, in resubmission order.
  std::size_t fresh = 0;
  for (TestGateway& gateway : gateways) {
    for (auto& [gatewaySequence, record] : gateway.unacked) {
      std::vector<char> copy = record;
      const std::uint64_t before = leader.sequence();
      leader.onSubmission(copy.data(), copy.size());
      fresh += leader.sequence() > before ? 1 : 0;
    }
    gateway.covered(acks[gateway.id].captured());
  }
  leader.flush();
  CHECK(fresh == flow.size() - replicated);
  CHECK(leader.sequence() == flow.size());
  for (const TestGateway& gateway : gateways) {
    CHECK(gateway.unacked.empty());
  }

  // The stitch: epoch one covered 1..180, epoch two continues from 181 with the resubmitted
  // tail; contiguous, complete, and the same book either way.
  Consumer consumer;
  Rewinder rewinder(standbyPath.string(), 2);
  consumer.feed(epochOnePackets, rewinder);
  consumer.feed(packets.packets, rewinder);
  CHECK(consumer.delivered.size() > 0);
  holdTheProperty(consumer.delivered, standbyPath.string());

  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}

TEST_CASE("a packet lost across the failover boundary is rewound from the surviving journal") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(107, 200);
  const std::filesystem::path primaryPath = scratch("boundary.exj");
  const std::filesystem::path standbyPath = scratch("boundary.standby.exj");
  std::vector<TestGateway> gateways{{0}};
  std::vector<std::vector<char>> epochOnePackets;
  Inherited inherited;

  {
    Wired old(primaryPath.string(), 1, Durability::SAFE);
    CapturingRing standbyAcks;
    common::journal::Writer standbyJournal(standbyPath.string());
    Standby<CapturingRing, common::journal::Writer> standby(standbyJournal, standbyAcks, 1);
    for (std::size_t at = 0; at < flow.size(); at++) {
      std::vector<char> record = gateways[0].submit(flow[at].bytes);
      old.sequencer.onSubmission(record.data(), record.size());
    }
    for (std::vector<char>& range : old.link.ranges) {
      standby.onRange(range.data(), range.size());
    }
    old.sequencer.onReplicationAck(flow.size());
    old.sequencer.heartbeat();
    standby.onRange(old.link.ranges.back().data(), old.link.ranges.back().size());
    epochOnePackets = old.packets.packets;
    inherited = standby.inherited();
  }

  CapturingRing out;
  std::vector<CapturingRing> acks(1);
  CapturingPacketSink packets;
  CapturingLink link;
  common::journal::Writer journal(standbyPath.string(), true);
  ScriptedClock stamps;
  Sequencer<CapturingRing, CapturingRing, CapturingPacketSink, common::journal::Writer,
            ScriptedClock, CapturingLink>
      leader(out, acks, packets, journal, stamps, link, Durability::LOCAL, 2);
  leader.inherit(inherited);
  leader.heartbeat();

  // The consumer loses the old leader's last data packet; the first thing it hears under epoch
  // two reveals the gap, and the rewinder serves it from the journal that survived.
  Consumer consumer;
  Rewinder rewinder(standbyPath.string(), 2);
  std::vector<std::vector<char>> lossy = epochOnePackets;
  lossy.pop_back();
  lossy.pop_back();
  consumer.feed(lossy, rewinder);
  consumer.feed(packets.packets, rewinder);
  CHECK(consumer.delivered == stamped(flow));

  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}
