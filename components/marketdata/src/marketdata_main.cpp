// The market data process: the matcher's events in through a ring, the public feed out as
// MoldUDP64-shaped packets on the A and B addresses, retransmission served over UDP, and
// snapshots served to late joiners over one-shot TCP connections in Glimpse's shape, each
// snapshot a sequence of length-prefixed packets ending with the sequence to join live at.
//
//   marketdata --in RING --a HOST:PORT --b HOST:PORT [--rewind PORT] [--glimpse PORT]
//              [--conflate-ms N] [--once]
//
// --once exits after serving one snapshot, which is what lets the integration test hold the
// whole process to its word.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "builder.hpp"
#include "clock.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/RewindRequest.h"
#include "glimpse.hpp"
#include "publisher.hpp"

namespace {

namespace common = exchange::common;
namespace market = exchange::marketdata;
namespace sbe = exchange::protocol;

volatile std::sig_atomic_t stopped = 0;

void onSignal(int) { stopped = 1; }

class UdpSink {
 public:
  UdpSink(const std::string& address) {
    const std::size_t colon = address.find(':');
    if (colon == std::string::npos) {
      return;
    }
    socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    to_.sin_family = AF_INET;
    to_.sin_port = htons(static_cast<std::uint16_t>(std::stoi(address.substr(colon + 1))));
    ::inet_pton(AF_INET, address.substr(0, colon).c_str(), &to_.sin_addr);
    // A multicast feed is ITCH's delivery: every participant joins the group and one send
    // reaches them all. The venue is a one-box deployment, so the group rides loopback; picking
    // a NIC is off-box work and lives on the horizon page.
    if ((ntohl(to_.sin_addr.s_addr) >> 28) == 0xE) {
      in_addr on{};
      on.s_addr = htonl(INADDR_LOOPBACK);
      ::setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_IF, &on, sizeof on);
      const unsigned char loop = 1;
      ::setsockopt(socket_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof loop);
    }
  }
  ~UdpSink() {
    if (socket_ >= 0) {
      ::close(socket_);
    }
  }
  void send(const char* bytes, const std::size_t length) {
    if (socket_ >= 0) {
      ::sendto(socket_, bytes, length, 0, reinterpret_cast<const sockaddr*>(&to_), sizeof to_);
    }
  }

 private:
  int socket_ = -1;
  sockaddr_in to_{};
};

std::string argument(const int count, char** values, const std::string& name) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return "";
}

bool flagged(const int count, char** values, const std::string& name) {
  for (int at = 1; at < count; at++) {
    if (name == values[at]) {
      return true;
    }
  }
  return false;
}

}  // namespace

int main(const int count, char** values) {
  const std::string inPath = argument(count, values, "--in");
  const std::string aAddress = argument(count, values, "--a");
  const std::string bAddress = argument(count, values, "--b");
  const std::string rewindPort = argument(count, values, "--rewind");
  const std::string glimpsePort = argument(count, values, "--glimpse");
  const std::string conflate = argument(count, values, "--conflate-ms");
  const bool once = flagged(count, values, "--once");
  if (inPath.empty() || aAddress.empty() || bAddress.empty()) {
    std::fprintf(stderr,
                 "usage: marketdata --in RING --a HOST:PORT --b HOST:PORT [--rewind PORT]"
                 " [--glimpse PORT] [--conflate-ms N] [--once]\n");
    return 2;
  }

  // The event stream is a broadcast ring: this publisher holds one seat among the feed's readers.
  common::BroadcastReader in = [&] {
    for (int attempt = 0; attempt < 300; attempt++) {
      if (std::filesystem::exists(inPath)) {
        try {
          return common::BroadcastReader::attach(inPath);
        } catch (const std::exception&) {
        }
      }
      ::usleep(100'000);
    }
    throw std::runtime_error("gave up waiting for " + inPath);
  }();

  UdpSink a(aAddress);
  UdpSink b(bAddress);
  market::Publisher<UdpSink, UdpSink> publisher(a, b);
  market::Builder<market::Publisher<UdpSink, UdpSink>> builder(publisher);
  exchange::sequencer::WallClock wall;
  const std::uint64_t conflateEvery =
      (conflate.empty() ? 100 : std::stoull(conflate)) * 1'000'000ULL;

  int rewind = -1;
  if (!rewindPort.empty()) {
    rewind = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(std::stoi(rewindPort)));
    const int flags = 1;
    ::setsockopt(rewind, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof flags);
    if (::bind(rewind, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
      std::fprintf(stderr, "cannot bind the rewind port %s\n", rewindPort.c_str());
      return 2;
    }
  }
  int glimpse = -1;
  if (!glimpsePort.empty()) {
    glimpse = ::socket(AF_INET, SOCK_STREAM, 0);
    const int reuse = 1;
    ::setsockopt(glimpse, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<std::uint16_t>(std::stoi(glimpsePort)));
    if (::bind(glimpse, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0 ||
        ::listen(glimpse, 4) != 0) {
      std::fprintf(stderr, "cannot listen on the glimpse port %s\n", glimpsePort.c_str());
      return 2;
    }
  }

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::uint64_t lastSend = wall.now();
  std::uint64_t lastConflation = wall.now();
  char scratch[2048];

  while (stopped == 0) {
    const std::size_t consumed =
        in.poll([&](char* message, const std::size_t length) { builder.onEvent(message, length); });
    if (consumed > 0) {
      publisher.flush();
      lastSend = wall.now();
    }
    const std::uint64_t now = wall.now();
    if (now - lastConflation >= conflateEvery) {
      builder.onConflation();
      publisher.flush();
      lastConflation = now;
      lastSend = now;
    }
    if (now - lastSend > 100'000'000ULL) {
      publisher.heartbeat();
      lastSend = now;
    }

    if (rewind >= 0) {
      sockaddr_in asker{};
      socklen_t askerSize = sizeof asker;
      const long got = ::recvfrom(rewind, scratch, sizeof scratch, MSG_DONTWAIT,
                                  reinterpret_cast<sockaddr*>(&asker), &askerSize);
      if (got > 0) {
        sbe::MessageHeader wrap;
        wrap.wrap(scratch, 0, 0, static_cast<std::uint64_t>(got));
        if (wrap.templateId() == sbe::RewindRequest::sbeTemplateId()) {
          sbe::RewindRequest request;
          request.wrapForDecode(scratch, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                                static_cast<std::uint64_t>(got));
          publisher.serveRewind(request.firstSequence(), request.count(),
                                [&](char* packet, const std::size_t length) {
                                  ::sendto(rewind, packet, length, 0,
                                           reinterpret_cast<const sockaddr*>(&asker), askerSize);
                                });
        }
      }
    }

    if (glimpse >= 0) {
      pollfd waiting{glimpse, POLLIN, 0};
      // A zero timeout keeps the event loop honest: snapshots are served between polls.
      if (::poll(&waiting, 1, 0) > 0 && (waiting.revents & POLLIN) != 0) {
        const int joiner = ::accept(glimpse, nullptr, nullptr);
        if (joiner >= 0) {
          const int nodelay = 1;
          ::setsockopt(joiner, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
          market::snapshot(builder, publisher.next(), [&](char* packet, const std::size_t length) {
            const std::uint32_t size = static_cast<std::uint32_t>(length);
            ::write(joiner, &size, sizeof size);
            ::write(joiner, packet, length);
          });
          ::close(joiner);
          if (once) {
            stopped = 1;
          }
        }
      }
    }
  }
  if (glimpse >= 0) {
    ::close(glimpse);
  }
  if (rewind >= 0) {
    ::close(rewind);
  }
  return 0;
}
