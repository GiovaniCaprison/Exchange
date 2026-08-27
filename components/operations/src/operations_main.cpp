// The market operations process: the scheduler live, submitting SessionControl through its own
// gateway carrier, consuming the event stream for its bands and confirmations, and answering
// the operator's signals. SIGUSR1 halts every instrument by the same machinery the bands use;
// SIGTERM lets the calendar's close stand as the last word.
//
//   operations --submissions RING --acks RING --events RING --instruments 1,2
//              [--calendar OFFSET:STATE,...] [--band-bps N] [--halt-ms N] [--auction-ms N]
//              [--gateway-id N]
//
// Calendar offsets are nanoseconds from process start; states are PRE_OPEN, OPENING_AUCTION,
// CONTINUOUS, CLOSING_AUCTION, HALTED and CLOSED. Real venues run calendars from reference data
// and an operations console; the flag is that console until the ecosystem phase builds one.

#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

#include "clock.hpp"
#include "scheduler.hpp"
#include "spsc_ring.hpp"

namespace {

namespace common = exchange::common;
namespace ops = exchange::operations;
namespace sbe = exchange::protocol;

volatile std::sig_atomic_t stopped = 0;
volatile std::sig_atomic_t haltAll = 0;

void onSignal(int) { stopped = 1; }
void onHalt(int) { haltAll = 1; }

std::string argument(const int count, char** values, const std::string& name) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return "";
}

std::vector<std::string> split(const std::string& list, const char by) {
  std::vector<std::string> parts;
  std::size_t from = 0;
  while (from <= list.size()) {
    const std::size_t mark = list.find(by, from);
    if (mark == std::string::npos) {
      parts.push_back(list.substr(from));
      break;
    }
    parts.push_back(list.substr(from, mark - from));
    from = mark + 1;
  }
  return parts;
}

sbe::SessionState::Value stateOf(const std::string& name) {
  if (name == "PRE_OPEN") {
    return sbe::SessionState::PRE_OPEN;
  }
  if (name == "OPENING_AUCTION") {
    return sbe::SessionState::OPENING_AUCTION;
  }
  if (name == "CONTINUOUS") {
    return sbe::SessionState::CONTINUOUS;
  }
  if (name == "CLOSING_AUCTION") {
    return sbe::SessionState::CLOSING_AUCTION;
  }
  if (name == "HALTED") {
    return sbe::SessionState::HALTED;
  }
  return sbe::SessionState::CLOSED;
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
  const std::string submissionsPath = argument(count, values, "--submissions");
  const std::string acksPath = argument(count, values, "--acks");
  const std::string eventsPath = argument(count, values, "--events");
  const std::string instruments = argument(count, values, "--instruments");
  const std::string calendar = argument(count, values, "--calendar");
  if (submissionsPath.empty() || acksPath.empty() || eventsPath.empty() || instruments.empty()) {
    std::fprintf(stderr,
                 "usage: operations --submissions RING --acks RING --events RING"
                 " --instruments 1,2 [--calendar OFFSET:STATE,...] [--band-bps N]"
                 " [--halt-ms N] [--auction-ms N] [--gateway-id N]\n");
    return 2;
  }

  common::SpscRing submissions = common::SpscRing::create(submissionsPath, 1 << 20);
  common::SpscRing acks = attachPatiently(acksPath);
  common::SpscRing events = attachPatiently(eventsPath);
  exchange::sequencer::WallClock wall;

  ops::Config config;
  for (const std::string& one : split(instruments, ',')) {
    config.instruments.push_back(static_cast<std::uint32_t>(std::stoul(one)));
  }
  const std::uint64_t start = wall.now();
  if (!calendar.empty()) {
    for (const std::string& entry : split(calendar, ',')) {
      const std::size_t colon = entry.find(':');
      config.calendar.push_back(
          {start + std::stoull(entry.substr(0, colon)), stateOf(entry.substr(colon + 1))});
    }
  }
  const std::string band = argument(count, values, "--band-bps");
  if (!band.empty()) {
    config.bandBasisPoints = std::stoll(band);
  }
  const std::string halt = argument(count, values, "--halt-ms");
  if (!halt.empty()) {
    config.haltNanos = std::stoull(halt) * 1'000'000ULL;
  }
  const std::string auction = argument(count, values, "--auction-ms");
  if (!auction.empty()) {
    config.auctionNanos = std::stoull(auction) * 1'000'000ULL;
  }
  const std::string gatewayId = argument(count, values, "--gateway-id");
  if (!gatewayId.empty()) {
    config.gatewayId = static_cast<std::uint32_t>(std::stoul(gatewayId));
  }
  const std::vector<std::uint32_t> everyone = config.instruments;

  ops::Scheduler<common::SpscRing, exchange::sequencer::WallClock> scheduler(submissions, wall,
                                                                             config);
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::signal(SIGUSR1, onHalt);

  while (stopped == 0) {
    if (haltAll != 0) {
      haltAll = 0;
      for (const std::uint32_t instrument : everyone) {
        scheduler.haltNow(instrument);
      }
    }
    acks.poll([&](char* message, const std::size_t length) { scheduler.onAck(message, length); });
    events.poll(
        [&](char* message, const std::size_t length) { scheduler.onEvent(message, length); });
    scheduler.onTick();
    ::usleep(1'000);
  }
  return 0;
}
