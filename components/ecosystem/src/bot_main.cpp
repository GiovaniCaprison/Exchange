// The participant process: one bot, seated at the venue over real wires. Order entry over TCP to
// the gateway, the public feed over UDP, unicast for a lone consumer or an ITCH-style multicast
// group every participant joins, retransmission asked of the rewinder over UDP, and the bot's
// decisions on top. With --results it writes the wire-to-wire series the campaign wants: order
// bytes written to acceptance heard, measured from the seat that pays for it, raw nanoseconds
// and an honest manifest (P-14).
//
//   bot --connect PORT --participant ID --secret S --feed PORT|GROUP:PORT [--rewind PORT]
//       --role maker|noise|momentum [--seed S] [--instrument N] [--duration-s N]
//       [--results DIR --label L] [--expect-trades] [--fair N]
//
// The bot connects patiently, trades for the duration, prints one summary line, and exits zero;
// with --expect-trades it exits one if it never traded, which is what lets an integration test
// hold a whole running venue to its word.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "bots.hpp"
#include "clock.hpp"
#include "exchange_protocol/RewindRequest.h"
#include "feedhandler.hpp"
#include "orderentry.hpp"

namespace {

namespace sbe = exchange::protocol;
using namespace exchange::ecosystem;

volatile std::sig_atomic_t stopped = 0;
void onSignal(int) { stopped = 1; }

std::string argument(const int count, char** values, const std::string& name,
                     const std::string& fallback) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return fallback;
}

bool flagged(const int count, char** values, const std::string& name) {
  for (int at = 1; at < count; at++) {
    if (name == values[at]) {
      return true;
    }
  }
  return false;
}

int connectPatiently(const std::uint16_t port) {
  for (int attempt = 0; attempt < 300; attempt++) {
    const int handle = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(handle, reinterpret_cast<const sockaddr*>(&address), sizeof address) == 0) {
      const int nodelay = 1;
      ::setsockopt(handle, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
      ::fcntl(handle, F_SETFL, ::fcntl(handle, F_GETFL, 0) | O_NONBLOCK);
      return handle;
    }
    ::close(handle);
    ::usleep(100'000);
  }
  return -1;
}

// The feed door: a port to bind, and when the address names a group, a membership to join, which
// is how every participant hears one multicast send.
int feedSocket(const std::string& spec) {
  const std::size_t colon = spec.find(':');
  const std::string group = colon == std::string::npos ? "" : spec.substr(0, colon);
  const std::uint16_t port = static_cast<std::uint16_t>(
      std::stoi(colon == std::string::npos ? spec : spec.substr(colon + 1)));
  const int handle = ::socket(AF_INET, SOCK_DGRAM, 0);
  const int reuse = 1;
  ::setsockopt(handle, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
#ifdef SO_REUSEPORT
  ::setsockopt(handle, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof reuse);
#endif
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);
  if (::bind(handle, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
    ::close(handle);
    return -1;
  }
  if (!group.empty()) {
    ip_mreq membership{};
    ::inet_pton(AF_INET, group.c_str(), &membership.imr_multiaddr);
    membership.imr_interface.s_addr = htonl(INADDR_LOOPBACK);
    if (::setsockopt(handle, IPPROTO_IP, IP_ADD_MEMBERSHIP, &membership, sizeof membership) != 0) {
      ::close(handle);
      return -1;
    }
  }
  ::fcntl(handle, F_SETFL, ::fcntl(handle, F_GETFL, 0) | O_NONBLOCK);
  return handle;
}

// The rewind channel over UDP: requests go to the retransmission server, and what comes back
// arrives on this same socket for the main loop to feed through the one door.
struct UdpRewind {
  int handle = -1;
  sockaddr_in to{};

  void open(const std::uint16_t port) {
    handle = ::socket(AF_INET, SOCK_DGRAM, 0);
    to.sin_family = AF_INET;
    to.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    to.sin_port = htons(port);
    ::fcntl(handle, F_SETFL, ::fcntl(handle, F_GETFL, 0) | O_NONBLOCK);
  }

  void request(const std::uint64_t firstSequence, const std::uint32_t count) {
    if (handle < 0) {
      return;
    }
    char space[64] = {};
    sbe::RewindRequest ask;
    ask.wrapAndApplyHeader(space, 0, sizeof space);
    ask.firstSequence(firstSequence).count(count).reserved(0);
    ::sendto(handle, space,
             sbe::MessageHeader::encodedLength() + sbe::RewindRequest::sbeBlockLength(), 0,
             reinterpret_cast<const sockaddr*>(&to), sizeof to);
  }
};

std::uint64_t quantile(const std::vector<std::uint64_t>& sorted, const double q) {
  if (sorted.empty()) {
    return 0;
  }
  const std::size_t at = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
  return sorted[at];
}

}  // namespace

int main(const int count, char** values) {
  const std::string connect = argument(count, values, "--connect", "");
  const std::string participant = argument(count, values, "--participant", "");
  const std::string secret = argument(count, values, "--secret", "");
  const std::string feedSpec = argument(count, values, "--feed", "");
  const std::string rewindPort = argument(count, values, "--rewind", "");
  const std::string role = argument(count, values, "--role", "");
  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "bot");
  const std::uint64_t seed = std::stoull(argument(count, values, "--seed", "1"));
  const std::uint32_t instrument =
      static_cast<std::uint32_t>(std::stoul(argument(count, values, "--instrument", "1")));
  const std::int64_t fairValue = std::stoll(argument(count, values, "--fair", "100000"));
  const std::uint64_t seconds = std::stoull(argument(count, values, "--duration-s", "10"));
  const bool expectTrades = flagged(count, values, "--expect-trades");
  if (connect.empty() || participant.empty() || secret.empty() || feedSpec.empty() ||
      role.empty()) {
    std::fprintf(stderr,
                 "usage: bot --connect PORT --participant ID --secret S --feed PORT|GROUP:PORT"
                 " --role maker|noise|momentum [--rewind PORT] [--seed S] [--instrument N]"
                 " [--duration-s N] [--results DIR --label L] [--expect-trades] [--fair N]\n");
    return 2;
  }

  const int venue = connectPatiently(static_cast<std::uint16_t>(std::stoi(connect)));
  const int feedIn = feedSocket(feedSpec);
  if (venue < 0 || feedIn < 0) {
    std::fprintf(stderr, "cannot reach the venue\n");
    return 2;
  }
  UdpRewind rewind;
  if (!rewindPort.empty()) {
    rewind.open(static_cast<std::uint16_t>(std::stoi(rewindPort)));
  }

  exchange::sequencer::WallClock clock;
  FeedHandler<UdpRewind> feed(rewind);
  OrderEntry<exchange::sequencer::WallClock> entry(
      clock, static_cast<std::uint32_t>(std::stoul(participant)), std::stoull(secret));
  entry.connected();

  Maker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>>::Config makerConfig;
  makerConfig.instrumentId = instrument;
  makerConfig.fairValue = fairValue;
  Maker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>> maker(feed, entry,
                                                                                  makerConfig);
  NoiseTaker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>>::Config
      noiseConfig;
  noiseConfig.instrumentId = instrument;
  NoiseTaker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>> noise(
      feed, entry, noiseConfig, seed);
  MomentumTaker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>>::Config
      momentumConfig;
  momentumConfig.instrumentId = instrument;
  MomentumTaker<FeedHandler<UdpRewind>, OrderEntry<exchange::sequencer::WallClock>> momentum(
      feed, entry, momentumConfig);

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  std::vector<std::uint64_t> latencies;
  latencies.reserve(1 << 20);
  char scratch[1 << 16];
  const std::uint64_t until = clock.now() + seconds * 1'000'000'000ULL;
  // Decisions run on a fixed cadence rather than at loop speed: an unpaced quoter re-quotes on
  // every wakeup, burns its rate-limit bucket at the gate, and livelocks on refusals. Two
  // hundred decisions a second per side sits well inside the venue's throttle.
  constexpr std::uint64_t CADENCE = 5'000'000;
  std::uint64_t lastDecision = 0;

  while (stopped == 0 && clock.now() < until) {
    pollfd waiting[3] = {{venue, POLLIN, 0}, {feedIn, POLLIN, 0}, {rewind.handle, POLLIN, 0}};
    ::poll(waiting, rewind.handle >= 0 ? 3 : 2, 1);

    long got = 0;
    while ((got = ::read(venue, scratch, sizeof scratch)) > 0) {
      entry.received(scratch, static_cast<std::size_t>(got));
    }
    if (got == 0) {
      break;  // The venue hung up.
    }
    while ((got = ::recv(feedIn, scratch, sizeof scratch, 0)) > 0) {
      feed.onPacket(scratch, static_cast<std::size_t>(got));
    }
    if (rewind.handle >= 0) {
      while ((got = ::recv(rewind.handle, scratch, sizeof scratch, 0)) > 0) {
        feed.onPacket(scratch, static_cast<std::size_t>(got));
      }
    }

    if (clock.now() - lastDecision >= CADENCE) {
      lastDecision = clock.now();
      if (role == "maker") {
        maker.onTick();
      } else if (role == "noise") {
        noise.onTick();
      } else {
        momentum.onTick();
      }
      entry.onTick();
    }

    const auto [bytes, length] = entry.outbound();
    if (length > 0) {
      const long wrote = ::write(venue, bytes, length);
      if (wrote > 0) {
        entry.drainedBy(static_cast<std::size_t>(wrote));
      }
    }
    for (std::uint64_t took = entry.drainAcceptanceLatency(); took != 0;
         took = entry.drainAcceptanceLatency()) {
      latencies.push_back(took);
    }
  }

  if (!resultsPath.empty() && !latencies.empty()) {
    std::filesystem::create_directories(resultsPath);
    const std::filesystem::path directory(resultsPath);
    std::vector<std::uint64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());
    {
      std::ofstream raw(directory / (label + "-timings.bin"), std::ios::binary);
      raw.write(reinterpret_cast<const char*>(latencies.data()),
                static_cast<long>(latencies.size() * sizeof(std::uint64_t)));
    }
    std::ofstream manifest(directory / (label + "-manifest.json"));
    manifest << "{\n"
             << "  \"label\": \"" << label << "\",\n"
             << "  \"path\": \"order written to TCP, acceptance heard back, the client's wire"
             << " to wire\",\n"
             << "  \"role\": \"" << role << "\",\n"
             << "  \"accepted\": " << latencies.size() << ",\n"
             << "  \"p50\": " << quantile(sorted, 0.50) << ",\n"
             << "  \"p99\": " << quantile(sorted, 0.99) << ",\n"
             << "  \"p999\": " << quantile(sorted, 0.999) << ",\n"
             << "  \"max\": " << (sorted.empty() ? 0 : sorted.back()) << "\n"
             << "}\n";
  }

  std::printf(
      "%s (%s): accepted %zu, executions %llu, rejections %llu, refusals %llu,"
      " position %lld, feed at %llu\n",
      label.c_str(), role.c_str(), latencies.size(),
      static_cast<unsigned long long>(entry.executions()),
      static_cast<unsigned long long>(entry.rejections()),
      static_cast<unsigned long long>(entry.refusals()),
      static_cast<long long>(entry.positionOf(instrument)),
      static_cast<unsigned long long>(feed.nextSequence()));
  ::close(venue);
  ::close(feedIn);
  if (expectTrades && entry.executions() == 0) {
    std::fprintf(stderr, "expected trades and saw none\n");
    return 1;
  }
  return 0;
}
