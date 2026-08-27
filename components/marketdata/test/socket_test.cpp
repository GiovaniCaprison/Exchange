// The whole process held to its word: events into its ring, packets out of its UDP feeds, and a
// late joiner served a snapshot over TCP that names the live sequence, with --once turning the
// served snapshot into the process's own exit. The test plays the venue and the joiner both.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "exchange_protocol/SnapshotComplete.h"
#include "feedtest.hpp"
#include "ranges.hpp"
#include "spsc_ring.hpp"

using namespace exchange::marketdata;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-marketdata-socket-" + name);
}

}  // namespace

TEST_CASE("the process serves packets on its feeds and snapshots to late joiners") {
  const std::filesystem::path ring = scratch("events.ring");
  const std::uint16_t udpA = 36711;
  const std::uint16_t udpB = 36712;
  const std::uint16_t glimpsePort = 36713;

  // The test is the A feed's audience: a bound UDP socket.
  const int listenerA = ::socket(AF_INET, SOCK_DGRAM, 0);
  sockaddr_in aAddress{};
  aAddress.sin_family = AF_INET;
  aAddress.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  aAddress.sin_port = htons(udpA);
  REQUIRE(::bind(listenerA, reinterpret_cast<const sockaddr*>(&aAddress), sizeof aAddress) == 0);

  common::SpscRing events = common::SpscRing::create(ring.string(), 1 << 20);

  const std::string binary = MARKETDATA_BINARY;
  REQUIRE(std::system((binary + " --in " + ring.string() + " --a 127.0.0.1:" +
                       std::to_string(udpA) + " --b 127.0.0.1:" + std::to_string(udpB) +
                       " --glimpse " + std::to_string(glimpsePort) + " --once &")
                          .c_str()) == 0);

  // The venue speaks: a real matcher run's events into the ring.
  const std::vector<CommandWriter::Framed> flow = generatedFlow(89, 400);
  CapturingRing captured;
  exchange::matcher::Partition<CapturingRing> partition(captured);
  for (const CommandWriter::Framed& framed : flow) {
    std::vector<char> bytes = framed.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  }
  std::vector<char> all = captured.captured();
  eachMessage(all, [&](char* message, const std::size_t length) {
    const std::size_t at = events.claim(length);
    std::memcpy(events.buffer() + at, message, length);
    events.commit();
    events.publish();
  });

  // A packet arrives on the A feed.
  char packet[2048];
  long got = -1;
  for (int attempt = 0; attempt < 300 && got <= 0; attempt++) {
    got = ::recv(listenerA, packet, sizeof packet, MSG_DONTWAIT);
    if (got <= 0) {
      ::usleep(10'000);
    }
  }
  REQUIRE(got > 0);
  common::ranges::Reader reader(packet, static_cast<std::size_t>(got));
  CHECK(reader.epoch() == 1);

  // The late joiner connects and receives the snapshot, closed by the join sequence.
  int joiner = -1;
  for (int attempt = 0; attempt < 100 && joiner < 0; attempt++) {
    const int trying = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(glimpsePort);
    if (::connect(trying, reinterpret_cast<const sockaddr*>(&address), sizeof address) == 0) {
      joiner = trying;
    } else {
      ::close(trying);
      ::usleep(100'000);
    }
  }
  REQUIRE(joiner >= 0);

  std::vector<char> collected;
  char chunk[4096];
  long piece = 0;
  while ((piece = ::read(joiner, chunk, sizeof chunk)) > 0) {
    collected.insert(collected.end(), chunk, chunk + piece);
  }
  ::close(joiner);
  REQUIRE(collected.size() > 8);

  std::uint64_t joinAt = 0;
  std::size_t orders = 0;
  std::size_t at = 0;
  while (at + sizeof(std::uint32_t) <= collected.size()) {
    std::uint32_t size = 0;
    std::memcpy(&size, collected.data() + at, sizeof size);
    at += sizeof size;
    common::ranges::Reader snapshotReader(collected.data() + at, size);
    snapshotReader.forEach([&](char* message, const std::size_t length) {
      exchange::protocol::MessageHeader wrap;
      wrap.wrap(message, 0, 0, length);
      if (wrap.templateId() == exchange::protocol::SnapshotComplete::sbeTemplateId()) {
        exchange::protocol::SnapshotComplete complete;
        complete.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                               length);
        joinAt = complete.nextSequence();
      }
      if (wrap.templateId() == exchange::protocol::PublicOrderAdded::sbeTemplateId()) {
        orders++;
      }
    });
    at += size;
  }
  CHECK(joinAt > 1);
  CHECK(orders > 0);

  ::close(listenerA);
  std::filesystem::remove(ring);
}
