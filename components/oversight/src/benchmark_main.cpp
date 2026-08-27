// The measurement harness for oversight: the cost per watched event, ownership, history and
// judgment included, timed from event bytes in to the verdict, warm-up excluded, raw series and
// manifest as everywhere (P-14). Surveillance sits off the latency path by design; the number
// exists so that claim is a measurement rather than a hope.
//
//   oversight-benchmark --results DIR [--events N] [--warmup N] [--core N] [--label L]

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

#include "surveillance.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

struct DiscardingSink {
  std::uint64_t alerts = 0;
  void operator()(char*, const std::size_t) { alerts++; }
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
  using namespace exchange::oversight;

  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "oversight");
  const std::uint64_t events = std::stoull(argument(count, values, "--events", "500000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (resultsPath.empty() || events <= warmup) {
    std::fprintf(stderr,
                 "usage: oversight-benchmark --results DIR [--events N] [--warmup N] [--core N]"
                 " [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  DiscardingSink sink;
  Config config;
  config.windowNanos = 64;
  Surveillance<DiscardingSink> watch(sink, config);

  // The measured turn is the busiest honest shape: accept, rest, cross, cancel, five events
  // whose middle three drive fills, judgments and table churn.
  char accepted[128] = {};
  char rested[128] = {};
  char takerAccepted[128] = {};
  char executed[128] = {};
  char removed[128] = {};
  const auto sizes = [] {
    struct Sizes {
      std::size_t accepted;
      std::size_t rested;
      std::size_t executed;
      std::size_t removed;
    } s{};
    s.accepted = sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength();
    s.rested = sbe::MessageHeader::encodedLength() + sbe::OrderRested::sbeBlockLength();
    s.executed = sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength();
    s.removed = sbe::MessageHeader::encodedLength() + sbe::OrderRemoved::sbeBlockLength();
    return s;
  }();

  std::vector<std::uint64_t> timings(events, 0);
  for (std::uint64_t at = 0; at < events; at++) {
    const std::uint64_t restingId = at * 2 + 1;
    const std::uint64_t takerId = at * 2 + 2;
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(accepted, 0, sizeof accepted);
      event.context().sequence(at * 5 + 1).inputSequence(at).timestamp(at * 4).instrumentId(1);
      event.orderId(restingId).clientOrderId(restingId).participantId(7);
    }
    {
      sbe::OrderRested event;
      event.wrapAndApplyHeader(rested, 0, sizeof rested);
      event.context().sequence(at * 5 + 2).inputSequence(at).timestamp(at * 4 + 1).instrumentId(1);
      event.orderId(restingId).price(10'000).quantity(5);
      event.side(sbe::Side::BUY);
    }
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(takerAccepted, 0, sizeof takerAccepted);
      event.context().sequence(at * 5 + 3).inputSequence(at).timestamp(at * 4 + 2).instrumentId(1);
      event.orderId(takerId).clientOrderId(takerId).participantId(8);
    }
    {
      sbe::OrderExecuted event;
      event.wrapAndApplyHeader(executed, 0, sizeof executed);
      event.context().sequence(at * 5 + 4).inputSequence(at).timestamp(at * 4 + 3).instrumentId(1);
      event.executionId(at).aggressorOrderId(takerId).restingOrderId(restingId);
      event.price(10'000).quantity(4);
    }
    {
      sbe::OrderRemoved event;
      event.wrapAndApplyHeader(removed, 0, sizeof removed);
      event.context().sequence(at * 5 + 5).inputSequence(at).timestamp(at * 4 + 4).instrumentId(1);
      event.orderId(restingId).quantity(1);
      event.reason(sbe::RemoveReason::CANCELLED);
    }
    const std::uint64_t before = now();
    watch.onEvent(accepted, sizes.accepted);
    watch.onEvent(rested, sizes.rested);
    watch.onEvent(takerAccepted, sizes.accepted);
    watch.onEvent(executed, sizes.executed);
    watch.onEvent(removed, sizes.removed);
    timings[at] = (now() - before) / 5;
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
             << "  \"path\": \"one event watched: ownership, history and judgment\",\n"
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
      "%s: %zu turns, per event p50 %llu ns, p99 %llu ns, p99.9 %llu ns, max %llu ns"
      " (%s)\n",
      label.c_str(), measured.size(), static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
