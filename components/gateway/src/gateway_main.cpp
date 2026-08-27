// The gateway process: sockets outside, the machine inside. One event loop owns the listener,
// every connection, and the venue's rings: on Linux the loop is epoll, level triggered with
// nonblocking sockets, which is the edge real order entry runs; elsewhere the same shape runs on
// poll(2), and ci exercising the socket suite on Linux is what keeps the epoll path honest.
// io_uring is the same edge again with batched syscalls and stays a named upgrade in the
// component page. TCP_NODELAY because a venue never trades latency for batching a client did
// not ask for.
//
//   gateway --listen PORT --submissions RING --acks RING --events RING
//           --participants ID:SECRET[,ID:SECRET...] [--gateway-id N] [--once] [--spin]
//
// The gateway creates its submission ring and attaches the acknowledgment and event rings, which
// the sequencer and matcher create, retrying until they exist so the processes can start in any
// order that respects who creates what. --once exits after the first established session ends,
// which is what lets an integration test hold the whole process to its word.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/epoll.h>
#endif

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
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

// The event stream is a broadcast ring: this gateway holds one seat among the feed's readers.
common::BroadcastReader joinPatiently(const std::string& path) {
  for (int attempt = 0; attempt < 300; attempt++) {
    if (std::filesystem::exists(path)) {
      try {
        return common::BroadcastReader::attach(path);
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
  // The rings cannot wake a socket wait: an acceptance sitting in shared memory would age for
  // the whole timeout while the loop sleeps. Real order gateways busy poll; --spin is that, and
  // the campaign pins the core it burns. The default keeps a laptop's fans honest.
  const bool spin = flagged(count, values, "--spin");
  const int patience = spin ? 0 : 10;
  if (listen.empty() || submissionsPath.empty() || acksPath.empty() || eventsPath.empty() ||
      participants.empty()) {
    std::fprintf(stderr,
                 "usage: gateway --listen PORT --submissions RING --acks RING --events RING"
                 " --participants ID:SECRET[,...] [--gateway-id N] [--once]\n");
    return 2;
  }

  common::SpscRing submissions = common::SpscRing::create(submissionsPath, 1 << 22);
  common::SpscRing acks = attachPatiently(acksPath);
  common::BroadcastReader events = joinPatiently(eventsPath);
  exchange::sequencer::WallClock clock;
  // One limits shape for every participant until the operations phase brings real config; the
  // defaults are permissive and the flag overrides them venue wide.
  exchange::risk::Limits limits;
  const std::string configured = argument(count, values, "--credit");
  if (!configured.empty()) {
    limits.credit = std::stoll(configured);
  }
  std::vector<std::pair<std::uint32_t, exchange::risk::Limits>> riskTable;
  for (const gate::Credential& credential : credentialsOf(participants)) {
    riskTable.emplace_back(credential.participantId, limits);
  }
  exchange::risk::Risk<exchange::sequencer::WallClock> risk(clock, riskTable);
  gate::Gateway<common::SpscRing, exchange::sequencer::WallClock,
                exchange::risk::Risk<exchange::sequencer::WallClock>>
      gateway(submissions, clock, risk,
              gatewayId.empty() ? 0 : static_cast<std::uint32_t>(std::stoul(gatewayId)),
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

  // The loop's verbs, shared by both waiting mechanisms.
  const auto acceptOne = [&]() -> int {
    const int accepted = ::accept(listener, nullptr, nullptr);
    if (accepted < 0) {
      return -1;
    }
    const int nodelay = 1;
    ::setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
    ::fcntl(accepted, F_SETFL, ::fcntl(accepted, F_GETFL, 0) | O_NONBLOCK);
    const int slot = gateway.opened();
    if (slot < 0) {
      ::close(accepted);
      return -1;
    }
    sockets[slot] = accepted;
    return slot;
  };
  const auto readable = [&](const int slot) {
    const long got = ::read(sockets[slot], scratch, sizeof scratch);
    if (got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
      ::close(sockets[slot]);
      sockets[slot] = -1;
      gateway.closed(slot);
      return;
    }
    if (got > 0) {
      gateway.received(slot, scratch, static_cast<std::size_t>(got));
    }
  };
  const auto writable = [&](const int slot) {
    const auto [bytes, length] = gateway.outbound(slot);
    if (length > 0) {
      const long wrote = ::write(sockets[slot], bytes, length);
      if (wrote > 0) {
        gateway.drained(slot, static_cast<std::size_t>(wrote));
      }
    }
  };
  const auto sweep = [&] {
    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] >= 0 && gateway.wantsClose(static_cast<int>(slot))) {
        ::close(sockets[slot]);
        sockets[slot] = -1;
        gateway.closed(static_cast<int>(slot));
        if (once) {
          stopped = 1;
        }
      }
    }
    acks.poll([&](char* message, const std::size_t length) { gateway.onAck(message, length); });
    events.poll([&](char* message, const std::size_t length) { gateway.onEvent(message, length); });
    gateway.onTick();
  };

#if defined(__linux__)
  // Level-triggered epoll over nonblocking sockets: readiness wakes the loop, writes are
  // attempted for whoever holds outbound bytes, and EAGAIN is a stalled client's problem
  // bounded by its own buffer cap rather than the loop's.
  const int waiter = ::epoll_create1(0);
  epoll_event interest{};
  interest.events = EPOLLIN;
  interest.data.fd = listener;
  ::epoll_ctl(waiter, EPOLL_CTL_ADD, listener, &interest);
  while (stopped == 0) {
    epoll_event woke[SLOTS + 1];
    const int ready = ::epoll_wait(waiter, woke, SLOTS + 1, patience);
    for (int at = 0; at < ready; at++) {
      if (woke[at].data.fd == listener) {
        const int slot = acceptOne();
        if (slot >= 0) {
          epoll_event watch{};
          watch.events = EPOLLIN;
          watch.data.u64 = 0x100000000ULL | static_cast<std::uint64_t>(slot);
          ::epoll_ctl(waiter, EPOLL_CTL_ADD, sockets[slot], &watch);
        }
      } else {
        const int slot = static_cast<int>(woke[at].data.u64 & 0xFFFFFFFFULL);
        if (sockets[slot] >= 0) {
          readable(slot);
        }
      }
    }
    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] >= 0) {
        writable(static_cast<int>(slot));
      }
    }
    sweep();
  }
  ::close(waiter);
#else
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
    ::poll(polled, entries, patience);
    if ((polled[0].revents & POLLIN) != 0) {
      acceptOne();
    }
    for (nfds_t at = 1; at < entries; at++) {
      const int slot = slotOf[at];
      if ((polled[at].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
        readable(slot);
      }
      if (sockets[slot] >= 0 && (polled[at].revents & POLLOUT) != 0) {
        writable(slot);
      }
    }
    sweep();
  }
#endif
  ::close(listener);
  return 0;
}
