// The hostile-bytes suite: the reassembler and the session machine fed what the internet
// actually sends, seeded garbage in random shreds, truncations, lengths that lie in both
// directions, and valid frames wrapping nonsense. The properties held are narrow and absolute:
// the process never breaks (the sanitizer flavour gives this teeth), nothing unauthorised is
// ever forwarded, and a stream that steps outside the framing is poisoned rather than argued
// with. The fuzz target in this directory feeds the same surface without a fixed seed.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "gateway.hpp"
#include "submission.hpp"
#include "wire.hpp"

using namespace exchange::gateway;
using namespace exchange::gateway::test;
using exchange::sequencer::VirtualClock;
using exchange::sequencer::test::CapturingLink;

namespace {

std::uint64_t next(std::uint64_t& state) {
  state ^= state >> 12;
  state ^= state << 25;
  state ^= state >> 27;
  return state * 2685821657736338717ULL;
}

}  // namespace

TEST_CASE("seeded garbage in random shreds never breaks the machine or reaches the venue") {
  CapturingLink submissions;
  VirtualClock clock;
  Gateway<CapturingLink, VirtualClock> gateway(submissions, clock, 1, {{7, 42}});
  std::uint64_t state = 20260827;

  for (int round = 0; round < 200; round++) {
    const int slot = gateway.opened();
    REQUIRE(slot >= 0);
    std::vector<char> garbage(1 + next(state) % 600);
    for (char& byte : garbage) {
      byte = static_cast<char>(next(state));
    }
    std::size_t fed = 0;
    while (fed < garbage.size()) {
      const std::size_t shred = 1 + next(state) % 32;
      const std::size_t take = shred < garbage.size() - fed ? shred : garbage.size() - fed;
      gateway.received(slot, garbage.data() + fed, take);
      fed += take;
    }
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
    gateway.closed(slot);
  }
  CHECK(gateway.submitted() == 0);
}

TEST_CASE("lengths that lie are poison in both directions") {
  CapturingLink submissions;
  VirtualClock clock;
  Gateway<CapturingLink, VirtualClock> gateway(submissions, clock, 1, {{7, 42}});

  // Shorter than any message header.
  const int tiny = gateway.opened();
  const char understated[] = {0x04, 0x00, 1, 2, 3, 4};
  gateway.received(tiny, understated, sizeof understated);
  CHECK(gateway.poisoned() == 1);

  // Longer than the cap.
  const int huge = gateway.opened();
  const std::uint16_t oversized = Reassembler::CAP + 1;
  char overstated[2];
  std::memcpy(overstated, &oversized, sizeof oversized);
  gateway.received(huge, overstated, sizeof overstated);
  CHECK(gateway.poisoned() == 2);
  CHECK(gateway.submitted() == 0);
}

TEST_CASE("a truncated frame waits forever and the clock ends it, never the parser") {
  CapturingLink submissions;
  VirtualClock clock;
  Gateway<CapturingLink, VirtualClock> gateway(submissions, clock, 1, {{7, 42}});
  const int slot = gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size() - 5);
  CHECK(gateway.poisoned() == 0);
  CHECK(gateway.rejections() == 0);
  clock.advance(6'000'000'000ULL);
  gateway.onTick();
  CHECK(gateway.timeouts() == 1);
}

TEST_CASE("a valid frame wrapping a forged command still cannot cross identity") {
  CapturingLink submissions;
  VirtualClock clock;
  Gateway<CapturingLink, VirtualClock> gateway(submissions, clock, 1, {{7, 42}, {8, 43}});
  const int slot = gateway.opened();
  std::vector<char> login = loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size());

  // A syntactically perfect cancel claiming participant 8 for an order participant 8 owns:
  // the stamp rewrites it to 7, and the matcher's ownership checks do the rest.
  CommandWriter writer;
  std::vector<char> forged = commandBytes(writer.cancel(1, 800, 8));
  gateway.received(slot, forged.data(), forged.size());
  REQUIRE(submissions.ranges.size() == 1);
  namespace sbe = exchange::protocol;
  char* command = submissions.ranges[0].data() + exchange::sequencer::SUBMISSION_BYTES;
  sbe::MessageHeader wrap;
  wrap.wrap(command, 0, 0, submissions.ranges[0].size());
  sbe::CancelOrder decoded;
  decoded.wrapForDecode(command, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                        submissions.ranges[0].size());
  CHECK(decoded.participantId() == 7);
}
