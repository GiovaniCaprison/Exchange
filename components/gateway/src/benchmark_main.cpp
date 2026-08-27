// The measurement harness for the machine between the wire and the ring: one logged-in session,
// prebuilt order frames in, submissions out, timed per message from bytes handed to the session
// to the record published, warm-up excluded, raw series and manifest written (P-14). The socket
// itself is deliberately outside this clock; the wire's own hop is the box's number to take,
// measured by the two-hop driver against the live process (docs/PRACTICE.md, the campaign).
//
//   gateway-benchmark --results DIR [--commands N] [--warmup N] [--core N] [--label L]

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif
#include <sys/utsname.h>

#include "client.hpp"
#include "clock.hpp"
#include "gateway.hpp"

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
  namespace gate = exchange::gateway;
  namespace test = exchange::gateway::test;
  namespace sbe = exchange::protocol;

  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "gateway");
  const std::uint64_t commands = std::stoull(argument(count, values, "--commands", "200000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (resultsPath.empty() || commands <= warmup) {
    std::fprintf(stderr,
                 "usage: gateway-benchmark --results DIR [--commands N] [--warmup N] [--core N]"
                 " [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  exchange::matcher::test::CommandWriter writer;
  std::vector<std::vector<char>> orders;
  orders.reserve(commands);
  for (std::uint64_t at = 0; at < commands; at++) {
    orders.push_back(test::commandBytes(
        writer.newOrder(1, 900 + at, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                        sbe::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 1, 0, 0, 0, 0)));
  }
  char ack[64] = {};
  {
    sbe::CommandSequenced sequenced;
    sequenced.wrapAndApplyHeader(ack, 0, sizeof ack);
    sequenced.gatewaySequence(0).sequence(0).timestamp(0);
  }

  DiscardingRing submissions;
  exchange::sequencer::ScriptedClock clock;
  gate::Gateway<DiscardingRing, exchange::sequencer::ScriptedClock> gateway(submissions, clock, 1,
                                                                            {{7, 42}});
  const int slot = gateway.opened();
  std::vector<char> login = test::loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size());
  const auto drainAll = [&] {
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
  };
  drainAll();

  std::vector<std::uint64_t> timings(commands, 0);
  for (std::uint64_t at = 0; at < commands; at++) {
    const std::uint64_t before = now();
    gateway.received(slot, orders[at].data(), orders[at].size());
    timings[at] = now() - before;
    const std::uint64_t gatewaySequence = at + 1;
    std::memcpy(ack + sbe::MessageHeader::encodedLength(), &gatewaySequence,
                sizeof gatewaySequence);
    gateway.onAck(ack, sizeof ack);
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
             << "  \"path\": \"session bytes in to submission published, socket excluded\",\n"
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
      "%s: %zu commands, p50 %llu ns, p99 %llu ns, p99.9 %llu ns, max %llu ns (%s)\n",
      label.c_str(), measured.size(), static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
