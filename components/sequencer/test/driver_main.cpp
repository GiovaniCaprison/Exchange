// The two-hop driver: the venue measured as a client would feel it, on one box. The driver plays
// a gateway, writing submissions into the gateway ring, and plays a consumer, reading the
// matcher's events; one command's latency is the nanoseconds from writing its submission to
// seeing the first event that answers it, which spans the gateway carrier, the sequencer under
// whichever durability policy it runs, the sequenced stream, and the matcher. Closed loop on
// purpose: one command in flight at a time measures the path rather than the queue, and what an
// offered load does to the queue is a different question the harness family answers separately.
//
//   driver --gateway RING --acks RING --events RING --results DIR
//          [--commands N] [--warmup N] [--seed S] [--core N] [--label L]
//
// The driver starts first and creates the gateway ring; the sequencer attaches it and creates
// the acknowledgment ring; the matcher creates the event ring; the driver retries attaching both
// until they exist, so the four processes can start in any order that respects who creates what.

#include <unistd.h>

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

#include "exchange_protocol/CommandSequenced.h"
#include "exchange_protocol/InstrumentDefinition.h"
#include "exchange_protocol/MessageHeader.h"
#include "flow.hpp"
#include "spsc_ring.hpp"
#include "submission.hpp"

#ifndef EXCHANGE_BUILD_FLAGS
#define EXCHANGE_BUILD_FLAGS "unrecorded"
#endif

namespace {

namespace common = exchange::common;
namespace test = exchange::sequencer::test;
namespace sbe = exchange::protocol;

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

// The other processes create these after the driver starts, so attaching retries.
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
  const std::string gatewayPath = argument(count, values, "--gateway", "");
  const std::string acksPath = argument(count, values, "--acks", "");
  const std::string eventsPath = argument(count, values, "--events", "");
  const std::string resultsPath = argument(count, values, "--results", "");
  const std::string label = argument(count, values, "--label", "path");
  const std::uint64_t commands = std::stoull(argument(count, values, "--commands", "100000"));
  const std::uint64_t warmup = std::stoull(argument(count, values, "--warmup", "10000"));
  const std::uint64_t seed = std::stoull(argument(count, values, "--seed", "20260827"));
  const long core = std::stol(argument(count, values, "--core", "-1"));
  if (gatewayPath.empty() || acksPath.empty() || eventsPath.empty() || resultsPath.empty()) {
    std::fprintf(stderr,
                 "usage: driver --gateway RING --acks RING --events RING --results DIR\n"
                 "              [--commands N] [--warmup N] [--seed S] [--core N] [--label L]\n");
    return 2;
  }

  const std::string isolation = pinned(core);
  const std::vector<exchange::matcher::test::CommandWriter::Framed> flow =
      exchange::matcher::test::generatedFlow(seed, commands);

  common::SpscRing gateway = common::SpscRing::create(gatewayPath, 1 << 22);
  common::SpscRing acks = attachPatiently(acksPath);
  common::SpscRing events = attachPatiently(eventsPath);

  // Closed loop: write submission k, then poll until the stream has answered it. The
  // acknowledgment carries the (gatewaySequence, sequence) mapping; events carry the input
  // sequence they answer, and a high-water mark over them makes the wait immune to an event
  // arriving before its own acknowledgment has been read. The one command the matcher answers
  // with silence is the instrument definition, so its wait ends at the acknowledgment.
  std::vector<std::uint64_t> timings(flow.size(), 0);
  std::uint64_t sequenceOfMine = 0;
  std::uint64_t answeredUpTo = 0;
  for (std::size_t at = 0; at < flow.size(); at++) {
    const std::vector<char> record =
        test::submissionRecord(0, static_cast<std::uint64_t>(at) + 1, flow[at].bytes);
    sbe::MessageHeader commandWrap;
    std::vector<char> command = flow[at].bytes;
    commandWrap.wrap(command.data(), 0, 0, command.size());
    const bool silent = commandWrap.templateId() == sbe::InstrumentDefinition::sbeTemplateId();
    sequenceOfMine = 0;
    const std::uint64_t before = now();
    const std::size_t claimed = gateway.claim(record.size());
    std::memcpy(gateway.buffer() + claimed, record.data(), record.size());
    gateway.commit();
    gateway.publish();
    while (sequenceOfMine == 0 || (!silent && answeredUpTo < sequenceOfMine)) {
      acks.poll([&](char* message, const std::size_t length) {
        sbe::MessageHeader wrap;
        wrap.wrap(message, 0, 0, length);
        sbe::CommandSequenced ack;
        ack.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          length);
        if (ack.gatewaySequence() == static_cast<std::uint64_t>(at) + 1) {
          sequenceOfMine = ack.sequence();
        }
      });
      events.poll([&](char* message, const std::size_t) {
        // Every event's context opens sequence then inputSequence; the attribution is a load.
        std::uint64_t inputSequence = 0;
        std::memcpy(&inputSequence,
                    message + sbe::MessageHeader::encodedLength() + sizeof(std::uint64_t),
                    sizeof inputSequence);
        answeredUpTo = inputSequence > answeredUpTo ? inputSequence : answeredUpTo;
      });
    }
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
             << "  \"path\": \"driver write to first answering event, closed loop\",\n"
             << "  \"commands\": " << flow.size() << ",\n"
             << "  \"warmup\": " << warmup << ",\n"
             << "  \"seed\": " << seed << ",\n"
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
