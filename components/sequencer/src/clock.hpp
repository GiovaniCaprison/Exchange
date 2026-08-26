// The venue's one clock (P-3): the sequencer reads it once per sequenced command and no process
// behind the sequencer reads any. The scripted clock is a pure function of how many commands
// were stamped, so a deterministic run stamps the same nanosecond twice and the suites can hold
// output bytes equal across runs; its values match the stamping the test harnesses write, which
// is what lets a sequenced stream be diffed against a hand-stamped one.

#pragma once

#include <cstdint>
#include <ctime>

namespace exchange::sequencer {

struct WallClock {
  std::uint64_t now() {
    timespec time{};
    clock_gettime(CLOCK_REALTIME, &time);
    return static_cast<std::uint64_t>(time.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(time.tv_nsec);
  }
};

class ScriptedClock {
 public:
  std::uint64_t now() { return 1'000'000'000'000ULL + ++reads_; }

 private:
  std::uint64_t reads_ = 0;
};

// Time the tests own: leases and failure detection rest on timeouts, and a timeout on a real
// clock is a flaky test, so the lease machinery runs on this under the chaos suite and every
// expiry is a fact the test scripted rather than a race it won.
class VirtualClock {
 public:
  std::uint64_t now() const { return now_; }
  void advance(const std::uint64_t nanoseconds) { now_ += nanoseconds; }

 private:
  std::uint64_t now_ = 1'000'000'000'000ULL;
};

}  // namespace exchange::sequencer
