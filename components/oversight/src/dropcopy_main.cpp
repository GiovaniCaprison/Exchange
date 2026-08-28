// The drop copy process: sockets outside, the machine inside, the same division the gateway
// keeps. Watchers connect over TCP, log in naming the participant they are entitled to watch,
// and hear that participant's events byte exactly. This plane is cold by design, so the loop is
// poll(2) everywhere: nothing here belongs on an isolated core.
//
//   dropcopy --listen PORT --events RING --watchers SCOPE:SECRET[,SCOPE:SECRET...] [--once]

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

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
#include "dropcopy.hpp"
#include "stream.hpp"

namespace {

namespace common = exchange::common;
namespace watch = exchange::oversight;

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

std::vector<watch::Watcher> watchersOf(const std::string& list) {
  std::vector<watch::Watcher> watchers;
  std::size_t from = 0;
  while (from < list.size()) {
    const std::size_t comma = list.find(',', from);
    const std::string entry =
        list.substr(from, comma == std::string::npos ? std::string::npos : comma - from);
    const std::size_t colon = entry.find(':');
    watchers.push_back({static_cast<std::uint32_t>(std::stoul(entry.substr(0, colon))),
                        std::stoull(entry.substr(colon + 1))});
    if (comma == std::string::npos) {
      break;
    }
    from = comma + 1;
  }
  return watchers;
}

// The event stream is a broadcast ring: drop copy holds one seat among the feed's readers.
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
  const std::string eventsPath = argument(count, values, "--events");
  const std::string watchers = argument(count, values, "--watchers");
  const bool once = flagged(count, values, "--once");
  if (listen.empty() || eventsPath.empty() || watchers.empty()) {
    std::fprintf(stderr,
                 "usage: dropcopy --listen PORT --events RING"
                 " --watchers SCOPE:SECRET[,...] [--once]\n");
    return 2;
  }

  // One seat per shard, each seat holding every twin of that shard's event stream: shards are
  // separated by commas, twins within a shard by a pipe, and the seat deduplicates by the event
  // sequence, so a twin matcher dying is a non-event.
  std::vector<common::stream::SequencedSeat> events;
  {
    std::size_t at = 0;
    while (at < eventsPath.size()) {
      const std::size_t comma = eventsPath.find(',', at);
      const std::string group =
          eventsPath.substr(at, comma == std::string::npos ? std::string::npos : comma - at);
      common::stream::SequencedSeat seat;
      std::size_t twin = 0;
      while (twin < group.size()) {
        const std::size_t pipe = group.find('|', twin);
        seat.join(joinPatiently(
            group.substr(twin, pipe == std::string::npos ? std::string::npos : pipe - twin)));
        if (pipe == std::string::npos) {
          break;
        }
        twin = pipe + 1;
      }
      events.push_back(std::move(seat));
      if (comma == std::string::npos) {
        break;
      }
      at = comma + 1;
    }
  }
  exchange::sequencer::WallClock clock;
  watch::DropCopy<exchange::sequencer::WallClock> machine(clock, watchersOf(watchers));

  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  const int reuse = 1;
  ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(static_cast<std::uint16_t>(std::stoi(listen)));
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0 ||
      ::listen(listener, 8) != 0) {
    std::fprintf(stderr, "cannot listen on %s\n", listen.c_str());
    return 2;
  }

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  constexpr std::size_t SLOTS = watch::DropCopy<exchange::sequencer::WallClock>::CONNECTIONS;
  int sockets[SLOTS];
  for (int& handle : sockets) {
    handle = -1;
  }
  char scratch[1 << 16];

  while (stopped == 0) {
    pollfd polled[SLOTS + 1];
    nfds_t entries = 0;
    polled[entries++] = {listener, POLLIN, 0};
    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] >= 0) {
        const auto [bytes, length] = machine.outbound(static_cast<int>(slot));
        polled[entries++] = {sockets[slot], static_cast<short>(POLLIN | (length > 0 ? POLLOUT : 0)),
                             0};
      }
    }
    ::poll(polled, entries, 10);

    if ((polled[0].revents & POLLIN) != 0) {
      const int accepted = ::accept(listener, nullptr, nullptr);
      if (accepted >= 0) {
        const int nodelay = 1;
        ::setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof nodelay);
        ::fcntl(accepted, F_SETFL, ::fcntl(accepted, F_GETFL, 0) | O_NONBLOCK);
        const int slot = machine.opened();
        if (slot < 0) {
          ::close(accepted);
        } else {
          sockets[slot] = accepted;
        }
      }
    }
    for (std::size_t slot = 0; slot < SLOTS; slot++) {
      if (sockets[slot] < 0) {
        continue;
      }
      const long got = ::read(sockets[slot], scratch, sizeof scratch);
      if (got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        ::close(sockets[slot]);
        sockets[slot] = -1;
        machine.closed(static_cast<int>(slot));
        continue;
      }
      if (got > 0) {
        machine.received(static_cast<int>(slot), scratch, static_cast<std::size_t>(got));
      }
      const auto [bytes, length] = machine.outbound(static_cast<int>(slot));
      if (length > 0) {
        const long wrote = ::write(sockets[slot], bytes, length);
        if (wrote > 0) {
          machine.drained(static_cast<int>(slot), static_cast<std::size_t>(wrote));
        }
      }
      if (machine.wantsClose(static_cast<int>(slot))) {
        ::close(sockets[slot]);
        sockets[slot] = -1;
        machine.closed(static_cast<int>(slot));
        if (once) {
          stopped = 1;
        }
      }
    }
    for (common::stream::SequencedSeat& shard : events) {
      shard.poll(
          [&](char* message, const std::size_t length) { machine.onEvent(message, length); });
    }
    machine.onTick();
  }
  return 0;
}
