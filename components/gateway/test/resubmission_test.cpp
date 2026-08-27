// The failover contract from the client's chair: the gateway resubmits everything the dead
// primary never acknowledged, the new leader answers replicated retries from its inherited
// windows and sequences fresh ones exactly once, and the gateway's own acknowledgment watermark
// heals to cover it all. This drives the real sequencer and standby, the same machinery the
// chaos suite kills.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "flow.hpp"
#include "gateway.hpp"
#include "journal.hpp"
#include "standby.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using namespace exchange::sequencer;
using exchange::matcher::test::CommandWriter;
using exchange::sequencer::test::CapturingLink;
using exchange::sequencer::test::CapturingPacketSink;
using exchange::sequencer::test::CapturingRing;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-gateway-failover-" + name);
}

}  // namespace

TEST_CASE("after a failover the gateway resubmits and exactly-once holds end to end") {
  const std::filesystem::path primaryPath = scratch("primary.exj");
  const std::filesystem::path standbyPath = scratch("standby.exj");

  CapturingLink submissions;
  VirtualClock clock;
  exchange::risk::Risk<VirtualClock> risk(clock, {{7, {}}});
  Gateway<CapturingLink, VirtualClock, exchange::risk::Risk<VirtualClock>> gateway(
      submissions, clock, risk, 0, {{7, 42}});
  const int slot = gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size());

  CommandWriter writer;
  const std::uint64_t total = 20;
  for (std::uint64_t at = 0; at < total; at++) {
    std::vector<char> order = commandBytes(writer.newOrder(
        1, 900 + at, 7, exchange::protocol::Side::BUY, exchange::protocol::Pricing::LIMIT,
        exchange::protocol::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 1, 0, 0, 0, 0));
    gateway.received(slot, order.data(), order.size());
  }
  REQUIRE(gateway.submitted() == total);

  // The old primary sequences and replicates everything, but dies having processed the standby's
  // coverage only up to 12, so the gateway holds acknowledgments 1 through 12 and no more.
  Inherited inherited;
  {
    exchange::sequencer::test::Wired old(primaryPath.string(), 1, Durability::SAFE);
    CapturingRing standbyAcks;
    exchange::common::journal::Writer standbyJournal(standbyPath.string());
    Standby<CapturingRing, exchange::common::journal::Writer> standby(standbyJournal, standbyAcks,
                                                                      1);
    for (std::vector<char>& record : submissions.ranges) {
      old.sequencer.onSubmission(record.data(), record.size());
    }
    for (std::vector<char>& range : old.link.ranges) {
      standby.onRange(range.data(), range.size());
    }
    old.sequencer.onReplicationAck(12);
    // Hand the primary's acknowledgments to the gateway the way its ring would.
    std::size_t at = 0;
    const std::vector<char>& captured = old.acks[0].captured();
    while (at + ACK_BYTES <= captured.size()) {
      std::vector<char> one(captured.begin() + static_cast<long>(at),
                            captured.begin() + static_cast<long>(at + ACK_BYTES));
      gateway.onAck(one.data(), one.size());
      at += ACK_BYTES;
    }
    inherited = standby.inherited();
  }
  CHECK(gateway.acked() == 12);

  // The witness's business happens; the standby becomes the leader on its own journal.
  CapturingRing out;
  std::vector<CapturingRing> acks(1);
  CapturingPacketSink packets;
  CapturingLink link;
  exchange::common::journal::Writer journal(standbyPath.string(), true);
  ScriptedClock stamps;
  Sequencer<CapturingRing, CapturingRing, CapturingPacketSink, exchange::common::journal::Writer,
            ScriptedClock, CapturingLink>
      leader(out, acks, packets, journal, stamps, link, Durability::LOCAL, 2);
  leader.inherit(inherited);

  // Silence past the resubmission allowance: the gateway retries everything unacknowledged.
  const std::size_t alreadyShipped = submissions.ranges.size();
  clock.advance(600'000'000ULL);
  gateway.onTick();
  CHECK(gateway.resubmitted() == total - 12);

  for (std::size_t at = alreadyShipped; at < submissions.ranges.size(); at++) {
    leader.onSubmission(submissions.ranges[at].data(), submissions.ranges[at].size());
  }
  // Every retry was replicated before the death, so every one is answered from the inherited
  // windows and nothing is sequenced twice.
  CHECK(leader.sequence() == total);
  CHECK(leader.duplicates() == total - 12);

  std::size_t at = 0;
  const std::vector<char>& captured = acks[0].captured();
  while (at + ACK_BYTES <= captured.size()) {
    std::vector<char> one(captured.begin() + static_cast<long>(at),
                          captured.begin() + static_cast<long>(at + ACK_BYTES));
    gateway.onAck(one.data(), one.size());
    at += ACK_BYTES;
  }
  CHECK(gateway.acked() == total);

  std::filesystem::remove(primaryPath);
  std::filesystem::remove(standbyPath);
}
