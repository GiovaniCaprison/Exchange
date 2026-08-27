// The fuzz target (docs/components/gateway.md): the same surface the hostile suite feeds with a
// seed, fed here by a coverage-guided fuzzer with none. It needs a toolchain that ships the
// libFuzzer runtime: ci's Linux clang, the box's, or the pinned Homebrew LLVM on a laptop;
// Xcode's does not carry it. Ci's bounded pass per merge is the gate, and long campaigns run
// wherever there is time:
//
//   cmake -B build-fuzz -DEXCHANGE_FUZZ=ON -DCMAKE_CXX_COMPILER=clang++
//   cmake --build build-fuzz --target gateway-fuzz
//   build-fuzz/components/gateway/gateway-fuzz -runs=20000 -max_len=512
//
// The properties are the hostile suite's: never break, never forward what a session did not
// earn. The sanitizers ride along, so a finding is a stack trace rather than a mystery.

#include <cstddef>
#include <cstdint>
#include <vector>

#include "clock.hpp"
#include "gateway.hpp"

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

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  DiscardingRing submissions;
  exchange::sequencer::VirtualClock clock;
  exchange::risk::Risk<exchange::sequencer::VirtualClock> risk(clock, {{7, {}}});
  exchange::gateway::Gateway<DiscardingRing, exchange::sequencer::VirtualClock,
                             exchange::risk::Risk<exchange::sequencer::VirtualClock>>
      gateway(submissions, clock, risk, 1, {{7, 42}});
  const int slot = gateway.opened();
  // The first byte shreds the rest, so the fuzzer explores reassembly boundaries too.
  const std::size_t shred = size == 0 ? 1 : 1 + data[0] % 64;
  std::size_t at = 1;
  while (at < size) {
    const std::size_t take = shred < size - at ? shred : size - at;
    gateway.received(slot, reinterpret_cast<const char*>(data) + at, take);
    at += take;
  }
  gateway.onTick();
  return 0;
}
