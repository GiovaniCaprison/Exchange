// The measurement harness for the feed: a real matcher run's events replayed through the
// builder and publisher, timed per event from bytes in to packets batched, warm-up excluded,
// raw series and manifest written (P-14). The wire is outside this clock; the box measures the
// event-in to packet-out hop against the live process (docs/PRACTICE.md, the campaign).
//
//   marketdata-benchmark --results DIR [--commands N] [--warmup N] [--core N] [--label L]

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

#include "feedtest.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

struct DiscardingSink {
  void send(const char*, std::size_t) {}
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
  namespace market = exchange::marketdata;
  namespace test = exchange::marketdata::test;

  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "marketdata");
  const std::uint64_t commands = std::stoull(argument(count, values, "--commands", "100000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (resultsPath.empty()) {
    std::fprintf(stderr,
                 "usage: marketdata-benchmark --results DIR [--commands N] [--warmup N]"
                 " [--core N] [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  const std::vector<test::CommandWriter::Framed> flow =
      exchange::matcher::test::generatedFlow(97, commands);
  test::CapturingRing captured;
  exchange::matcher::Partition<test::CapturingRing> partition(captured);
  for (const test::CommandWriter::Framed& framed : flow) {
    std::vector<char> bytes = framed.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  }
  std::vector<char> bytes = captured.captured();
  std::vector<std::pair<std::size_t, std::size_t>> messages;
  test::eachMessage(bytes, [&](char* message, const std::size_t length) {
    messages.emplace_back(static_cast<std::size_t>(message - bytes.data()), length);
  });
  if (messages.size() <= warmup) {
    std::fprintf(stderr, "the run yielded %zu events and the warmup wants %llu\n", messages.size(),
                 static_cast<unsigned long long>(warmup));
    return 2;
  }

  DiscardingSink a;
  DiscardingSink b;
  market::Publisher<DiscardingSink, DiscardingSink> publisher(a, b);
  market::Builder<market::Publisher<DiscardingSink, DiscardingSink>> builder(publisher);

  std::vector<std::uint64_t> timings(messages.size(), 0);
  for (std::size_t at = 0; at < messages.size(); at++) {
    const std::uint64_t before = now();
    builder.onEvent(bytes.data() + messages[at].first, messages[at].second);
    timings[at] = now() - before;
  }
  publisher.flush();

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
             << "  \"path\": \"event bytes in to public message batched, wire excluded\",\n"
             << "  \"events\": " << messages.size() << ",\n"
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
      "%s: %zu events, p50 %llu ns, p99 %llu ns, p99.9 %llu ns, max %llu ns (%s)\n", label.c_str(),
      measured.size(), static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
