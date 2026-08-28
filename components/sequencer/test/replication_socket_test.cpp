// The replication link held to its word over real UDP: a primary process under the safe policy
// ships to a standby process on another port the way it would ship to another box, the repair
// conversation rides back on one socket, and when the session closes the standby's journal is
// the primary's, byte for byte. The wire here is loopback; the box campaign moves the standby a
// machine away and nothing in this test changes but the address.

#include <unistd.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "flow.hpp"
#include "spsc_ring.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-replication-socket-" + name);
}

std::vector<char> bytesOf(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}  // namespace

TEST_CASE("the safe policy crosses a real wire and the journals agree") {
  const std::filesystem::path room = scratch("room");
  std::filesystem::remove_all(room);
  std::filesystem::create_directories(room);
  const std::string in = room.string() + "/";
  const std::filesystem::path pids = room / "pids";

  const std::vector<CommandWriter::Framed> flow = generatedFlow(23, 1'500);
  std::vector<std::vector<char>> records = dealtSubmissions(flow, 1);

  // The test plays the gateway: it creates the submission ring both processes will meet at.
  common::SpscRing gateway = common::SpscRing::create(in + "gw.ring", 1 << 22);

  struct Reaper {
    std::string pids;
    ~Reaper() { std::system(("kill $(cat " + pids + ") 2>/dev/null; sleep 1").c_str()); }
  } reaper{pids.string()};
  const std::string binary = SEQUENCER_BINARY;
  REQUIRE(
      std::system((binary + " --standby-udp 36301 --repair-udp 127.0.0.1:36302 --journal " + in +
                   "standby.exj > " + in + "standby.log 2>&1 & echo $! >> " + pids.string())
                      .c_str()) == 0);
  REQUIRE(std::system((binary + " --in " + in + "gw.ring --acks " + in + "ack.ring --out " + in +
                       "seq.ring --journal " + in +
                       "primary.exj --policy safe --replicate-udp 127.0.0.1:36301"
                       " --repair-port 36302 > " +
                       in + "primary.log 2>&1 & echo $! >> " + pids.string())
                          .c_str()) == 0);

  for (std::vector<char>& record : records) {
    const std::size_t at = gateway.claim(record.size());
    std::memcpy(gateway.buffer() + at, record.data(), record.size());
    gateway.commit();
    gateway.publish();
  }

  // Every command acknowledged means every command replicated, published and answered.
  common::SpscRing acks = [&] {
    for (int attempt = 0; attempt < 300; attempt++) {
      if (std::filesystem::exists(in + "ack.ring")) {
        try {
          return common::SpscRing::attach(in + "ack.ring");
        } catch (const std::exception&) {
        }
      }
      ::usleep(100'000);
    }
    throw std::runtime_error("the primary never made its ack ring");
  }();
  std::size_t acknowledged = 0;
  for (int attempt = 0; attempt < 600 && acknowledged < flow.size(); attempt++) {
    acknowledged += acks.poll([](char*, std::size_t) {});
    ::usleep(10'000);
  }
  REQUIRE(acknowledged == flow.size());

  // A clean shutdown closes the session on both sides; the standby exits on the end marker.
  std::system(("kill -TERM $(head -2 " + pids.string() + " | tail -1) 2>/dev/null").c_str());
  for (int attempt = 0; attempt < 300; attempt++) {
    if (bytesOf(room / "standby.exj").size() == bytesOf(room / "primary.exj").size() &&
        !bytesOf(room / "primary.exj").empty()) {
      break;
    }
    ::usleep(100'000);
  }
  const std::vector<char> primary = bytesOf(room / "primary.exj");
  const std::vector<char> standby = bytesOf(room / "standby.exj");
  CHECK(!primary.empty());
  CHECK(primary == standby);
}
