// The post-trade process: the event stream in through its broadcast seat, the day out as the
// two files PROTOCOL.md documents. It runs quietly until every instrument the stream ever
// mentioned has closed, writes the trades and positions files, prints one line for the
// operator, and exits, because an end-of-day process that lingers is a question nobody wants at
// midnight. SIGTERM writes whatever the day holds so far and exits the same way.
//
//   posttrade --events RING --trades FILE --positions FILE

#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "ledger.hpp"
#include "stream.hpp"

namespace {

namespace common = exchange::common;

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

// The event stream is a broadcast ring: the ledger holds one seat among the feed's readers.
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
  const std::string eventsPath = argument(count, values, "--events");
  const std::string tradesPath = argument(count, values, "--trades");
  const std::string positionsPath = argument(count, values, "--positions");
  if (eventsPath.empty() || tradesPath.empty() || positionsPath.empty()) {
    std::fprintf(stderr, "usage: posttrade --events RING --trades FILE --positions FILE\n");
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
  exchange::posttrade::Ledger ledger;

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0 && !ledger.dayIsDone()) {
    std::size_t consumed = 0;
    for (common::stream::SequencedSeat& shard : events) {
      consumed += shard.poll(
          [&](char* message, const std::size_t length) { ledger.onEvent(message, length); });
    }
    if (consumed == 0) {
      ::usleep(1'000);
    }
  }

  {
    std::ofstream trades(tradesPath);
    ledger.writeTrades(trades);
  }
  {
    std::ofstream positions(positionsPath);
    ledger.writePositions(positions);
  }
  std::printf("the day is written: %zu trades, %zu accounts\n", ledger.trades().size(),
              ledger.accounts().size());
  return 0;
}
