// The measurement harness: replays a journal through a partition, timing each command from framed
// bytes in to last event out, warm-up excluded, and writes what a claim needs to be checked
// (P-14): the raw nanosecond series, the quantiles, and a manifest recording the machine, the
// build, the input and whether the isolation that was asked for actually took. The harness runs
// anywhere; numbers that mean anything come from the dedicated box, and the manifest is what
// tells the two kinds of run apart.
//
//   matcher-benchmark --journal J --results DIR [--warmup N] [--core N] [--label L]
//
// Timing wraps onCommand around a discarding ring, so the cost measured is decode, match and
// encode, the work a venue pays per command, without the capture the harness would otherwise
// spend. Core pinning is honoured where the platform offers it and recorded either way.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif
#include <sys/utsname.h>

#include "journal.hpp"
#include "partition.hpp"

namespace {

namespace matching = exchange::matcher;

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
  void commit() { events_++; }
  void publish() {}
  std::uint64_t events() const { return events_; }

 private:
  char space_[1 << 16] = {};
  std::size_t cursor_ = 0;
  std::uint64_t events_ = 0;
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
  const std::string journalPath = argument(count, values, "--journal", "");
  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "run");
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (journalPath.empty() || resultsPath.empty()) {
    std::fprintf(stderr,
                 "usage: matcher-benchmark --journal J --results DIR [--warmup N] [--core N]"
                 " [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  matching::journal::Read log = matching::journal::read(journalPath);
  if (log.count() <= warmup) {
    std::fprintf(stderr, "the journal holds %zu commands and the warmup wants %llu\n", log.count(),
                 static_cast<unsigned long long>(warmup));
    return 2;
  }

  DiscardingRing ring;
  matching::Partition<DiscardingRing> partition(ring);
  std::vector<std::uint64_t> timings(log.count(), 0);

  for (std::size_t at = 0; at < log.count(); at++) {
    const std::uint64_t before = now();
    partition.onCommand(log.messages.data() + log.offsets[at], 0, log.lengths[at]);
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
             << "  \"journal\": \"" << journalPath << "\",\n"
             << "  \"commands\": " << log.count() << ",\n"
             << "  \"warmup\": " << warmup << ",\n"
             << "  \"events\": " << ring.events() << ",\n"
             << "  \"isolation\": \"" << isolation << "\",\n"
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
