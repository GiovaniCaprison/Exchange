// The whole process held to its word over a real socket: a TCP client logs in, sends an order,
// and the gateway's submission ring carries it with the session's identity stamped; the venue's
// forged acceptance goes back down the event ring and arrives framed on the client's socket; a
// logout ends the session and, run with --once, the process itself. The test plays both the
// client and the venue, so the only untested seam left is the wire the box will measure.

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

#include "client.hpp"
#include "exchange_protocol/LoginAccepted.h"
#include "exchange_protocol/SessionEnded.h"
#include "flow.hpp"
#include "spsc_ring.hpp"
#include "submission.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
namespace common = exchange::common;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-gateway-socket-" + name);
}

int connectPatiently(const std::uint16_t port) {
  for (int attempt = 0; attempt < 100; attempt++) {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof address) == 0) {
      return socket;
    }
    ::close(socket);
    ::usleep(100'000);
  }
  return -1;
}

// Reads until the predicate says the collected messages are enough, or the peer hangs up.
template <typename Enough>
std::vector<std::pair<std::uint16_t, std::vector<char>>> readUntil(const int socket,
                                                                   Enough&& enough) {
  std::vector<char> collected;
  char chunk[4096];
  for (int attempt = 0; attempt < 200; attempt++) {
    const long got = ::read(socket, chunk, sizeof chunk);
    if (got <= 0) {
      break;
    }
    collected.insert(collected.end(), chunk, chunk + got);
    auto messages = unframed(collected.data(), collected.size());
    if (enough(messages)) {
      return messages;
    }
  }
  return unframed(collected.data(), collected.size());
}

}  // namespace

TEST_CASE("a client speaks to the process over TCP and the venue hears the truth") {
  const std::uint16_t port = 35711;
  const std::filesystem::path submissions = scratch("subs.ring");
  const std::filesystem::path acks = scratch("acks.ring");
  const std::filesystem::path events = scratch("events.ring");
  std::filesystem::remove(submissions);

  // The test is the venue: it creates the rings the sequencer and matcher would.
  common::SpscRing ackRing = common::SpscRing::create(acks.string(), 1 << 16);
  common::SpscRing eventRing = common::SpscRing::create(events.string(), 1 << 16);

  const std::string binary = GATEWAY_BINARY;
  REQUIRE(std::system((binary + " --listen " + std::to_string(port) + " --submissions " +
                       submissions.string() + " --acks " + acks.string() + " --events " +
                       events.string() + " --participants 7:42 --gateway-id 3 --once &")
                          .c_str()) == 0);

  const int client = connectPatiently(port);
  REQUIRE(client >= 0);

  std::vector<char> login = loginBytes(7, 42, 0);
  REQUIRE(::write(client, login.data(), login.size()) == static_cast<long>(login.size()));
  const auto accepted = readUntil(client, [](const auto& messages) { return !messages.empty(); });
  REQUIRE(accepted.size() == 1);
  CHECK(accepted[0].first == exchange::protocol::LoginAccepted::sbeTemplateId());

  // The order goes down the socket and must come out of the submission ring stamped.
  exchange::matcher::test::CommandWriter writer;
  std::vector<char> order = commandBytes(writer.newOrder(
      1, 900, 9, exchange::protocol::Side::BUY, exchange::protocol::Pricing::LIMIT,
      exchange::protocol::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 10, 0, 0, 0, 0));
  REQUIRE(::write(client, order.data(), order.size()) == static_cast<long>(order.size()));

  common::SpscRing submissionRing = [&] {
    for (int attempt = 0; attempt < 100; attempt++) {
      if (std::filesystem::exists(submissions)) {
        try {
          return common::SpscRing::attach(submissions.string());
        } catch (const std::exception&) {
        }
      }
      ::usleep(100'000);
    }
    throw std::runtime_error("the gateway never made its ring");
  }();
  std::vector<char> forwarded;
  for (int attempt = 0; attempt < 200 && forwarded.empty(); attempt++) {
    submissionRing.poll(
        [&](char* record, const std::size_t length) { forwarded.assign(record, record + length); });
    ::usleep(10'000);
  }
  REQUIRE(!forwarded.empty());
  namespace sbe = exchange::protocol;
  char* command = forwarded.data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, forwarded.size());
  sbe::NewOrder decoded;
  decoded.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        forwarded.size());
  CHECK(decoded.participantId() == 7);

  // The venue answers: an acceptance down the event ring arrives framed on the socket.
  std::vector<char> report = acceptedEvent(1, 7, 900);
  const std::size_t at = eventRing.claim(report.size());
  std::memcpy(eventRing.buffer() + at, report.data(), report.size());
  eventRing.commit();
  eventRing.publish();
  const auto reports = readUntil(client, [](const auto& messages) { return !messages.empty(); });
  REQUIRE(!reports.empty());
  CHECK(reports[0].first == exchange::protocol::OrderAccepted::sbeTemplateId());
  CHECK(reports[0].second == report);

  // A polite end, which --once turns into the process's own.
  std::vector<char> logout = logoutBytes();
  REQUIRE(::write(client, logout.data(), logout.size()) == static_cast<long>(logout.size()));
  const auto last = readUntil(client, [](const auto& messages) {
    return !messages.empty() &&
           messages.back().first == exchange::protocol::SessionEnded::sbeTemplateId();
  });
  REQUIRE(!last.empty());
  CHECK(last.back().first == exchange::protocol::SessionEnded::sbeTemplateId());
  ::close(client);

  std::filesystem::remove(submissions);
  std::filesystem::remove(acks);
  std::filesystem::remove(events);
}
