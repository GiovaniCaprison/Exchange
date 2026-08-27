// The measurement harness for the market's clock: the cost per monitored print, band
// maintenance included, timed from event bytes in to the decision made, warm-up excluded, raw
// series and manifest as everywhere (P-14). The submissions land in a discarding ring because
// the decision is the work; the carriers are priced by their own components.
//
//   operations-benchmark --results DIR [--events N] [--warmup N] [--core N] [--label L]

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif
#include <sys/utsname.h>

#include "clock.hpp"
#include "scheduler.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

class DiscardingRing {
 public:
  std::size_t claim(const std::size_t length) {
    const std::size_t aligned = (length + 7) & ~std::size_t{7};
    if (cursor_ + aligned > sizeof space_) {
      cursor_ = 0;
    }
    const std::size_t at = cursor_;
    cursor_ += aligned;
    return at;
  }
  char* buffer() { return space_; }
  void commit() {}
  void publish() {}

 private:
  char space_[1 << 16] = {};
  std::size_t cursor_ = 0;
};

std::string argument(const int count, char** values, const std::string& name,
                     const std::string& fallback) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return fallback;
}

std::uint64_t now() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

std::string pinned(const long core) {
#if defined(__linux__)
  if (core >= 0) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<int>(core), &set);
    if (sched_setaffinity(0, sizeof set, &set) == 0) {
      return "pinned to core " + std::to_string(core);
    }
    return "pin to core " + std::to_string(core) + " refused";
  }
  return "no core requested";
#else
  return core >= 0 ? "pinning is not available on this platform" : "no core requested";
#endif
}

std::uint64_t quantile(const std::vector<std::uint64_t>& sorted, const double q) {
  if (sorted.empty()) {
    return 0;
  }
  const std::size_t at = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
  return sorted[at];
}

}  // namespace

int main(const int count, char** values) {
  namespace sbe = exchange::protocol;
  using namespace exchange::operations;

  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "operations");
  const std::uint64_t events = std::stoull(argument(count, values, "--events", "500000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (resultsPath.empty() || events <= warmup) {
    std::fprintf(stderr,
                 "usage: operations-benchmark --results DIR [--events N] [--warmup N] [--core N]"
                 " [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  exchange::sequencer::VirtualClock wall;
  DiscardingRing out;
  Config config;
  config.instruments = {1};
  config.windowNanos = 10'000;
  Scheduler<DiscardingRing, exchange::sequencer::VirtualClock> scheduler(out, wall, config);

  char trading[128] = {};
  {
    sbe::SessionStateChanged began;
    began.wrapAndApplyHeader(trading, 0, sizeof trading);
    began.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
    began.state(sbe::SessionState::CONTINUOUS);
  }
  scheduler.onEvent(
      trading, sbe::MessageHeader::encodedLength() + sbe::SessionStateChanged::sbeBlockLength());

  char print[128] = {};
  {
    sbe::OrderExecuted event;
    event.wrapAndApplyHeader(print, 0, sizeof print);
    event.context().sequence(1).inputSequence(1).timestamp(0).instrumentId(1).reserved(0);
    event.executionId(1).aggressorOrderId(900).restingOrderId(901).price(10'000).quantity(1);
  }
  const std::size_t printSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength();

  std::vector<std::uint64_t> timings(events, 0);
  for (std::uint64_t at = 0; at < events; at++) {
    const std::uint64_t when = at + 1;
    const std::int64_t price = 10'000 + static_cast<std::int64_t>(at % 9);
    std::memcpy(print + sbe::MessageHeader::encodedLength() + 16, &when, sizeof when);
    std::memcpy(print + sbe::MessageHeader::encodedLength() + 56, &price, sizeof price);
    const std::uint64_t before = now();
    scheduler.onEvent(print, printSize);
    timings[at] = now() - before;
  }

  std::vector<std::uint64_t> measured(timings.begin() + static_cast<long>(warmup), timings.end());
  std::vector<std::uint64_t> sorted = measured;
  std::sort(sorted.begin(), sorted.end());

  std::filesystem::create_directories(resultsPath);
  const std::filesystem::path directory(resultsPath);
  {
    std::ofstream raw(directory / (label + "-timings.bin"), std::ios::binary);
    raw.write(reinterpret_cast<const char*>(measured.data()),
              static_cast<long>(measured.size() * sizeof(std::uint64_t)));
  }
  utsname machine{};
  uname(&machine);
  {
    std::ofstream manifest(directory / (label + "-manifest.json"));
    manifest << "{\n"
             << "  \"label\": \"" << label << "\",\n"
             << "  \"path\": \"one print monitored, band maintained, decision made\",\n"
             << "  \"events\": " << events << ",\n"
             << "  \"warmup\": " << warmup << ",\n"
             << "  \"isolation\": \"" << isolation << "\",\n"
             << "  \"build\": \"" << EXCHANGE_BUILD_FLAGS << "\",\n"
             << "  \"system\": \"" << machine.sysname << " " << machine.release << " "
             << machine.machine << "\",\n"
             << "  \"compiler\": \"" <<
#if defined(__clang__)
        "clang " << __clang_major__ << "." << __clang_minor__
#elif defined(__GNUC__)
        "gcc " << __GNUC__ << "." << __GNUC_MINOR__
#else
        "unknown"
#endif
             << "\",\n"
             << "  \"p50\": " << quantile(sorted, 0.50) << ",\n"
             << "  \"p99\": " << quantile(sorted, 0.99) << ",\n"
             << "  \"p999\": " << quantile(sorted, 0.999) << ",\n"
             << "  \"max\": " << (sorted.empty() ? 0 : sorted.back()) << "\n"
             << "}\n";
  }
  std::printf(
      "%s: %zu prints, p50 %llu ns, p99 %llu ns, p99.9 %llu ns, max %llu ns (%s)\n", label.c_str(),
      measured.size(), static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
