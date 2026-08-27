// The measurement the component page promises: the cost per admitted order, timed across the
// whole gate, throttle to credit, with the reconciling events fed between measurements so the
// ledger stays honest without riding the clock. Raw series and manifest as everywhere (P-14).
//
//   risk-benchmark --results DIR [--commands N] [--warmup N] [--core N] [--label L]

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
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderRemoved.h"
#include "risk.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

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
  using exchange::risk::Intent;
  using exchange::risk::Limits;
  using exchange::risk::Risk;

  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "risk");
  const std::uint64_t commands = std::stoull(argument(count, values, "--commands", "500000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (resultsPath.empty() || commands <= warmup) {
    std::fprintf(stderr,
                 "usage: risk-benchmark --results DIR [--commands N] [--warmup N] [--core N]"
                 " [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  exchange::sequencer::VirtualClock clock;
  Limits generous;
  generous.burst = 1'000'000;
  generous.collarWidth = 1'000'000;
  Risk<exchange::sequencer::VirtualClock> risk(clock, {{7, generous}});

  char accepted[128] = {};
  char removed[128] = {};
  {
    sbe::OrderAccepted event;
    event.wrapAndApplyHeader(accepted, 0, sizeof accepted);
    event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
    event.participantId(7);
    sbe::OrderRemoved gone;
    gone.wrapAndApplyHeader(removed, 0, sizeof removed);
    gone.context().sequence(2).inputSequence(2).timestamp(2).instrumentId(1).reserved(0);
    gone.quantity(1).reason(sbe::RemoveReason::CANCELLED);
  }
  const std::size_t acceptedSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength();
  const std::size_t removedSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderRemoved::sbeBlockLength();

  std::vector<std::uint64_t> timings(commands, 0);
  for (std::uint64_t at = 0; at < commands; at++) {
    clock.advance(1'000'000);
    Intent order{sbe::NewOrder::sbeTemplateId(),
                 900 + at,
                 1,
                 1000 + static_cast<std::int64_t>(at % 7),
                 5,
                 true,
                 false};
    const std::uint64_t before = now();
    const bool admitted = risk.admit(7, order).admitted;
    timings[at] = now() - before;
    if (!admitted) {
      std::fprintf(stderr, "the gate refused the benchmark's own flow\n");
      return 1;
    }
    const std::uint64_t orderId = at + 1;
    std::uint64_t clientOrderId = 900 + at;
    std::memcpy(accepted + sbe::MessageHeader::encodedLength() + 32, &orderId, sizeof orderId);
    std::memcpy(accepted + sbe::MessageHeader::encodedLength() + 40, &clientOrderId,
                sizeof clientOrderId);
    risk.onEvent(accepted, acceptedSize);
    std::memcpy(removed + sbe::MessageHeader::encodedLength() + 32, &orderId, sizeof orderId);
    risk.onEvent(removed, removedSize);
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
             << "  \"path\": \"one admission through every check, events off the clock\",\n"
             << "  \"commands\": " << commands << ",\n"
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
      "%s: %zu admissions, p50 %llu ns, p99 %llu ns, p99.9 %llu ns, max %llu ns (%s)\n",
      label.c_str(), measured.size(), static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
