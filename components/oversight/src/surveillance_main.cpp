// The surveillance process: the event stream in through its broadcast seat, alerts out to the
// alert journal in the standard journal format and one line each to stdout for the operator. A
// case is a range of sequence numbers in a replayable file, not a log line.
//
//   surveillance --events RING --alerts JOURNAL [--window-ms N] [--multiple N] [--levels N]

#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "exchange_protocol/SurveillanceAlert.h"
#include "journal.hpp"
#include "stream.hpp"
#include "surveillance.hpp"

namespace {

namespace common = exchange::common;
namespace sbe = exchange::protocol;

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

// The event stream is a broadcast ring: the watcher holds one seat among the feed's readers.
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

const char* nameOf(const sbe::AlertKind::Value kind) {
  switch (kind) {
    case sbe::AlertKind::WASH_TRADE:
      return "WASH_TRADE";
    case sbe::AlertKind::SPOOFING:
      return "SPOOFING";
    default:
      return "LAYERING";
  }
}

}  // namespace

int main(const int count, char** values) {
  const std::string eventsPath = argument(count, values, "--events");
  const std::string alertsPath = argument(count, values, "--alerts");
  if (eventsPath.empty() || alertsPath.empty()) {
    std::fprintf(stderr,
                 "usage: surveillance --events RING --alerts JOURNAL [--window-ms N]"
                 " [--multiple N] [--levels N]\n");
    return 2;
  }

  exchange::oversight::Config config;
  const std::string window = argument(count, values, "--window-ms");
  if (!window.empty()) {
    config.windowNanos = std::stoull(window) * 1'000'000ULL;
  }
  const std::string multiple = argument(count, values, "--multiple");
  if (!multiple.empty()) {
    config.spoofMultiple = std::stoll(multiple);
  }
  const std::string levels = argument(count, values, "--levels");
  if (!levels.empty()) {
    config.layeringLevels = static_cast<std::uint32_t>(std::stoul(levels));
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
  common::journal::Writer journal(alertsPath);
  auto sink = [&](char* bytes, const std::size_t length) {
    journal.append(bytes, static_cast<std::uint32_t>(length));
    sbe::MessageHeader wrap;
    wrap.wrap(bytes, 0, 0, length);
    sbe::SurveillanceAlert alert;
    alert.wrapForDecode(bytes, wrap.encodedLength(), wrap.blockLength(), wrap.version(), length);
    std::printf(
        "alert %llu %s participant %u instrument %u at sequence %llu:"
        " executed %lld, cancelled %lld over %u levels\n",
        static_cast<unsigned long long>(alert.alertId()), nameOf(alert.kind()),
        alert.participantId(), alert.instrumentId(),
        static_cast<unsigned long long>(alert.sequence()),
        static_cast<long long>(alert.executedQuantity()),
        static_cast<long long>(alert.cancelledQuantity()), alert.priceLevels());
    std::fflush(stdout);
  };
  exchange::oversight::Surveillance<decltype(sink)> watching(sink, config);

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0) {
    std::size_t consumed = 0;
    for (common::stream::SequencedSeat& shard : events) {
      consumed += shard.poll(
          [&](char* message, const std::size_t length) { watching.onEvent(message, length); });
    }
    if (consumed == 0) {
      ::usleep(1'000);
    }
  }
  return 0;
}
