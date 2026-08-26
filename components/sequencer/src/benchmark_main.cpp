// The measurement harness: replays a submission file through the sequencer, timing each
// submission from framed bytes in to acknowledgment out, warm-up excluded, and writes what a
// claim needs to be checked (P-14): the raw nanosecond series, the quantiles, and a manifest
// recording the machine, the durability policy and whether the isolation asked for actually
// took. Two policies exist so the price of durability is a measured number: local journals every
// command through the write path, and none sequences in memory alone, which brackets what the
// journal costs before replication changes the answer again.
//
//   sequencer-benchmark --submissions S --results DIR [--policy local|none] [--warmup N]
//                       [--core N] [--label L]

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

#include "clock.hpp"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "sequencer.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

namespace common = exchange::common;
namespace sequencing = exchange::sequencer;

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

struct DiscardingPacketSink {
  void send(const char*, std::size_t) {}
};

struct NullJournal {
  void append(const char*, std::uint32_t) {}
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

template <typename Journal>
std::vector<std::uint64_t> timed(common::journal::Read& submissions, Journal& journal,
                                 const std::uint32_t gateways) {
  DiscardingRing out;
  std::vector<DiscardingRing> acks(gateways);
  DiscardingPacketSink packets;
  sequencing::ScriptedClock clock;
  sequencing::Sequencer<DiscardingRing, DiscardingRing, DiscardingPacketSink, Journal,
                        sequencing::ScriptedClock>
      sequencer(out, acks, packets, journal, clock);
  std::vector<std::uint64_t> timings(submissions.count(), 0);
  for (std::size_t at = 0; at < submissions.count(); at++) {
    const std::uint64_t before = now();
    sequencer.onSubmission(submissions.messages.data() + submissions.offsets[at],
                           submissions.lengths[at]);
    timings[at] = now() - before;
  }
  sequencer.flush();
  return timings;
}

}  // namespace

int main(const int count, char** values) {
  const std::string submissionsPath = argument(count, values, "--submissions", "");
  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string policy = argument(count, values, "--policy", "local");
  const std::string label = argument(count, values, "--label", "run-" + policy);
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (submissionsPath.empty() || resultsPath.empty() || (policy != "local" && policy != "none")) {
    std::fprintf(stderr,
                 "usage: sequencer-benchmark --submissions S --results DIR"
                 " [--policy local|none] [--warmup N] [--core N] [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  common::journal::Read submissions = common::journal::read(submissionsPath);
  if (submissions.count() <= warmup) {
    std::fprintf(stderr, "the file holds %zu submissions and the warmup wants %llu\n",
                 submissions.count(), static_cast<unsigned long long>(warmup));
    return 2;
  }
  // Off the clock: size the gateway table from the file itself.
  std::uint32_t gateways = 1;
  for (std::size_t at = 0; at < submissions.count(); at++) {
    namespace sbe = exchange::protocol;
    sbe::MessageHeader wrap;
    wrap.wrap(submissions.messages.data() + submissions.offsets[at], 0, 0, submissions.lengths[at]);
    sbe::GatewaySubmission submission;
    submission.wrapForDecode(submissions.messages.data() + submissions.offsets[at],
                             wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                             submissions.lengths[at]);
    gateways = submission.gatewayId() + 1 > gateways ? submission.gatewayId() + 1 : gateways;
  }

  std::filesystem::create_directories(resultsPath);
  const std::filesystem::path directory(resultsPath);
  std::vector<std::uint64_t> timings;
  if (policy == "local") {
    common::journal::Writer journal((directory / (label + "-journal.exj")).string());
    timings = timed(submissions, journal, gateways);
  } else {
    NullJournal journal;
    timings = timed(submissions, journal, gateways);
  }

  std::vector<std::uint64_t> measured(timings.begin() + static_cast<long>(warmup), timings.end());
  std::vector<std::uint64_t> sorted = measured;
  std::sort(sorted.begin(), sorted.end());

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
             << "  \"submissions\": \"" << submissionsPath << "\",\n"
             << "  \"commands\": " << submissions.count() << ",\n"
             << "  \"warmup\": " << warmup << ",\n"
             << "  \"policy\": \"" << policy << "\",\n"
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
      "%s: %zu submissions, policy %s, p50 %llu ns, p99 %llu ns, p99.9 %llu ns,"
      " max %llu ns (%s)\n",
      label.c_str(), measured.size(), policy.c_str(),
      static_cast<unsigned long long>(quantile(sorted, 0.50)),
      static_cast<unsigned long long>(quantile(sorted, 0.99)),
      static_cast<unsigned long long>(quantile(sorted, 0.999)),
      static_cast<unsigned long long>(sorted.empty() ? 0 : sorted.back()), isolation.c_str());
  return 0;
}
