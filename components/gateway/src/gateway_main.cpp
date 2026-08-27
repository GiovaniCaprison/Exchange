// The gateway process: sockets outside, the machine inside. One poll(2) loop owns the listener,
// every connection, and the venue's rings, which is the portable statement of the single-threaded
// event loop; epoll and io_uring are the same shape with cheaper syscalls and arrive with the
// box work (docs/components/gateway.md). TCP_NODELAY because a venue never trades latency for
// batching a client did not ask for.
//
//   gateway --listen PORT --submissions RING --acks RING --events RING
//           --participants ID:SECRET[,ID:SECRET...] [--gateway-id N] [--once]
//
// The gateway creates its submission ring and attaches the acknowledgment and event rings, which
// the sequencer and matcher create, retrying until they exist so the processes can start in any
// order that respects who creates what. --once exits after the first established session ends,
// which is what lets an integration test hold the whole process to its word.

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

#include "clock.hpp"
#include "gateway.hpp"
#include "spsc_ring.hpp"

namespace {

namespace common = exchange::common;
namespace gate = exchange::gateway;

volatile std::sig_atomic_t stopped = 0;

void onSignal(int) { stopped = 1; }

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

std::vector<gate::Credential> credentialsOf(const std::string& list) {
  std::vector<gate::Credential> credentials;
  std::size_t from = 0;
  while (from < list.size()) {
    const std::size_t comma = list.find(',', from);
    const std::string entry =
        list.substr(from, comma == std::string::npos ? std::string::npos : comma - from);
    const std::size_t colon = entry.find(':');
    credentials.push_back({static_cast<std::uint32_t>(std::stoul(entry.substr(0, colon))),
                           std::stoull(entry.substr(colon + 1))});
    if (comma == std::string::npos) {
      break;
    }
    from = comma + 1;
  }
  return credentials;
}

common::SpscRing attachPatiently(const std::string& path) {
  for (int attempt = 0; attempt < 300; attempt++) {
    if (std::filesystem::exists(path)) {
      try {
        return common::SpscRing::attach(path);
      } catch (const std::exception&) {
      }
    }
    ::usleep(100'000);
  }
  throw std::runtime_error("gave up waiting for " + path);
}

}  // namespace

int main(const int count, char** values) {
  const std::string listen = argument(count, values, "--listen");
  const std::string submissionsPath = argument(count, values, "--submissions");
  const std::string acksPath = argument(count, values, "--acks");
  const std::string eventsPath = argument(count, values, "--events");
  const std::string participants = argument(count, values, "--participants");
  const std::string gatewayId = argument(count, values, "--gateway-id");
  const bool once = flagged(count, values, "--once");
  if (listen.empty() || submissionsPath.empty() || acksPath.empty() || eventsPath.empty() ||
      participants.empty()) {
    std::fprintf(stderr,
                 "usage: gateway --listen PORT --submissions RING --acks RING --events RING"
                 " --participants ID:SECRET[,...] [--gateway-id N] [--once]\n");
    return 2;
  }

  common::SpscRing submissions = common::SpscRing::create(submissionsPath, 1 << 22);
  common::SpscRing acks = attachPatiently(acksPath);
  common::SpscRing events = attachPatiently(eventsPath);
  exchange::sequencer::WallClock clock;
  gate::Gateway<common::SpscRing, exchange::sequencer::WallClock> gateway(
      submissions, clock, gatewayId.empty() ? 0 : static_cast<std::uint32_t>(std::stoul(gatewayId)),
      credentialsOf(participants));

  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  const int reuse = 1;
  ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(static_cast<std::uint16_t>(std::stoi(listen)));
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0 ||
      ::listen(listener, 16) != 0) {
    std::fprintf(stderr, "cannot listen on %s\n", listen.c_str());
    return 2;
  }

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  constexpr std::size_t SLOTS = 64;
  int sockets[SLOTS];
  for (int& socket : sockets) {
    socket = -1;
  }
  char scratch[1 << 16];

  while (stopped == 0) {
    pollfd polled[SLOTS + 1];
    polled[0] = {listener, POLLIN, 0};
    nfds_t entries = 1;
    int slotOf[SLOTS + 1];
    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] >= 0) {
        const auto [bytes, length] = gateway.outbound(static_cast<int>(slot));
        polled[entries] = {sockets[slot], static_cast<short>(POLLIN | (length > 0 ? POLLOUT : 0)),
                           0};
        slotOf[entries] = static_cast<int>(slot);
        entries++;
      }
    }
    ::poll(polled, entries, 10);

    if ((polled[0].revents & POLLIN) != 0) {
      const int accepted = ::accept(listener, nullptr, nullptr);
      if (accepted >= 0) {
        const int nodelay = 1;
        ::setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
        const int slot = gateway.opened();
        if (slot >= 0) {
          sockets[slot] = accepted;
        } else {
          ::close(accepted);
        }
      }
    }

    for (nfds_t at = 1; at < entries; at++) {
      const int slot = slotOf[at];
      if ((polled[at].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
        const long got = ::read(sockets[slot], scratch, sizeof scratch);
        if (got <= 0) {
          ::close(sockets[slot]);
          sockets[slot] = -1;
          gateway.closed(slot);
          continue;
        }
        gateway.received(slot, scratch, static_cast<std::size_t>(got));
      }
      if ((polled[at].revents & POLLOUT) != 0) {
        const auto [bytes, length] = gateway.outbound(slot);
        if (length > 0) {
          const long wrote = ::write(sockets[slot], bytes, length);
          if (wrote > 0) {
            gateway.drained(slot, static_cast<std::size_t>(wrote));
          }
        }
      }
    }

    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] >= 0) {
        if (gateway.wantsClose(static_cast<int>(slot))) {
          ::close(sockets[slot]);
          sockets[slot] = -1;
          gateway.closed(static_cast<int>(slot));
          if (once) {
            stopped = 1;
          }
        }
      }
    }

    acks.poll([&](char* message, const std::size_t length) { gateway.onAck(message, length); });
    events.poll([&](char* message, const std::size_t length) { gateway.onEvent(message, length); });
    gateway.onTick();
  }
  ::close(listener);
  return 0;
}
