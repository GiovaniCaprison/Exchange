// The matcher survives its own death, held over real processes and real wires: a primary
// matcher on the ring and a warm twin following the sequencer's packet feed through the
// rewinder, both journaling, both broadcasting. Their journals and their event streams are byte
// identical while both live; when the primary is killed mid-day the twin keeps deciding, and a
// consumer seated at both rings hears one unbroken stream across the death, exactly once and in
// order, which is the availability claim as a diff instead of a paragraph.

#include <unistd.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "flow.hpp"
#include "spsc_ring.hpp"
#include "stream.hpp"
#include "submission.hpp"

using exchange::matcher::test::CommandWriter;
using exchange::matcher::test::generatedFlow;
using exchange::sequencer::test::dealtSubmissions;
namespace common = exchange::common;
namespace sbe = exchange::protocol;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-failover-" + name);
}

std::vector<char> bytesOf(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}  // namespace

TEST_CASE("the twin keeps deciding when the primary dies, and the seat never notices") {
  const std::filesystem::path room = scratch("room");
  std::filesystem::remove_all(room);
  std::filesystem::create_directories(room);
  const std::string in = room.string() + "/";
  const std::filesystem::path pids = room / "pids";

  // A modest day on purpose: the sanitized battery runs this on a loaded machine, and the
  // claim is about the death, not the volume.
  const std::vector<CommandWriter::Framed> flow = generatedFlow(59, 1'000);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);

  common::SpscRing gateway = common::SpscRing::create(in + "gw.ring", 1 << 22);

  const std::string sequencer = SEQUENCER_BINARY;
  const std::string matcher = MATCHER_BINARY;
  // Daemons write to their own logs, never to this test's pipe, or a failure would leave ctest
  // reading a pipe the orphans hold open forever; and the reaper kills whatever is left however
  // this test ends, because a REQUIRE that throws must not leak a venue.
  struct Reaper {
    std::string pids;
    ~Reaper() { std::system(("kill $(cat " + pids + ") 2>/dev/null; sleep 1").c_str()); }
  } reaper{pids.string()};
  std::size_t launched = 0;
  const auto launch = [&](const std::string& command) {
    launched++;
    REQUIRE(std::system((command + " > " + in + "log" + std::to_string(launched) +
                         ".txt 2>&1 & echo $! >> " + pids.string())
                            .c_str()) == 0);
  };
  launch(sequencer + " --in " + in + "gw.ring --acks " + in + "ack.ring --out " + in +
         "seq.ring --journal " + in + "seq.exj --udp 127.0.0.1:36401");
  launch(sequencer + " --rewinder --journal " + in + "seq.exj --rewind-port 36402");
  launch(matcher + " --in " + in + "seq.ring --out " + in + "a.ring --journal " + in + "ma.exj");
  launch(matcher + " --follow-udp 36401 --repair-udp 127.0.0.1:36402 --out " + in +
         "b.ring --journal " + in + "mb.exj");

  // The first half of the day, with both twins alive.
  const std::size_t half = records.size() / 2;
  for (std::size_t at = 0; at < half; at++) {
    const std::size_t slot = gateway.claim(records[at].size());
    std::memcpy(gateway.buffer() + slot, records[at].data(), records[at].size());
    gateway.commit();
    gateway.publish();
  }

  // The consumer's chair: one seat over both rings, deduplicated by event sequence.
  common::stream::SequencedSeat seat;
  for (const char* ring : {"a.ring", "b.ring"}) {
    for (int attempt = 0; attempt < 300; attempt++) {
      if (std::filesystem::exists(in + ring)) {
        try {
          seat.join(common::BroadcastReader::attach(in + ring));
          break;
        } catch (const std::exception&) {
        }
      }
      ::usleep(100'000);
    }
  }
  REQUIRE(seat.twins() == 2);

  std::vector<char> heard;
  std::uint64_t last = 0;
  bool ordered = true;
  const auto listen = [&] {
    seat.poll([&](char* message, const std::size_t length) {
      std::uint64_t sequence = 0;
      std::memcpy(&sequence, message + sbe::MessageHeader::encodedLength(), sizeof sequence);
      ordered = ordered && sequence == last + 1;
      last = sequence;
      heard.insert(heard.end(), message, message + length);
    });
  };
  for (int attempt = 0; attempt < 600 && last == 0; attempt++) {
    listen();
    ::usleep(10'000);
  }
  REQUIRE(last > 0);

  // Both twins caught up on the first half: their journals agree before the death. The wait is
  // its own condition, generous because the battery shares this machine.
  bool agreed = false;
  for (int attempt = 0; attempt < 2'400 && !agreed; attempt++) {
    listen();
    const std::vector<char> primary = bytesOf(room / "ma.exj");
    agreed = !primary.empty() && primary == bytesOf(room / "mb.exj");
    ::usleep(50'000);
  }
  REQUIRE(agreed);
  const std::uint64_t beforeDeath = last;
  REQUIRE(beforeDeath > 0);

  // The death: the primary matcher, third launched, is killed without ceremony.
  std::system(("kill -KILL $(head -3 " + pids.string() + " | tail -1) 2>/dev/null").c_str());

  // The second half of the day: only the twin is deciding now.
  for (std::size_t at = half; at < records.size(); at++) {
    const std::size_t slot = gateway.claim(records[at].size());
    std::memcpy(gateway.buffer() + slot, records[at].data(), records[at].size());
    gateway.commit();
    gateway.publish();
  }
  std::uint64_t settled = 0;
  for (int attempt = 0; attempt < 2'400; attempt++) {
    listen();
    if (last > beforeDeath && last == settled) {
      break;
    }
    settled = last;
    ::usleep(50'000);
  }

  // The stream never broke: every event once, in order, across the death.
  CHECK(ordered);
  CHECK(last > beforeDeath);
  CHECK(!heard.empty());
}
