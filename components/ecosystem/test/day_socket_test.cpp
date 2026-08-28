// The whole venue held to its word over real wires: every process real and separate, the
// gateway, the sequencer, the matcher, the market data publisher and the operations scheduler,
// wired over rings the way the box runs them, with two bot processes seated over TCP and UDP. A
// dealer quotes, the crowd trades against it, and the crowd's own exit code is the assertion,
// because a bot told to expect trades exits nonzero without them. The dealer's wire-to-wire
// manifest is the campaign's measurement path proven end to end.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-day-" + name);
}

bool appears(const std::filesystem::path& path) {
  for (int attempt = 0; attempt < 100; attempt++) {
    if (std::filesystem::exists(path)) {
      return true;
    }
    ::system("sleep 0.1");
  }
  return false;
}

void launch(const std::string& command, const std::filesystem::path& pids) {
  // Daemons write to their own logs, never to this test's pipe, or a failure would leave ctest
  // reading a pipe the orphans hold open forever.
  static int voice = 0;
  REQUIRE(std::system((command + " > " + pids.parent_path().string() + "/log" +
                       std::to_string(++voice) + ".txt 2>&1 & echo $! >> " + pids.string())
                          .c_str()) == 0);
}

// Kills whatever is left however the test ends, because a REQUIRE that throws must not leak a
// venue.
struct Reaper {
  std::string pids;
  ~Reaper() { std::system(("kill $(cat " + pids + ") 2>/dev/null; sleep 1").c_str()); }
};

}  // namespace

TEST_CASE("the whole venue runs as processes and its participants trade over the wires") {
  const std::filesystem::path room = scratch("room");
  std::filesystem::remove_all(room);
  std::filesystem::create_directories(room);
  const std::filesystem::path pids = room / "pids";
  const std::string in = room.string() + "/";
  Reaper reaper{pids.string()};

  // The carriers who create their rings first, then everyone downstream in dependency order.
  launch(std::string(GATEWAY_BINARY) + " --listen 36101 --submissions " + in + "gw.ring --acks " +
             in + "gwacks.ring --events " + in +
             "events.ring --participants 7:42,8:43 --gateway-id 0",
         pids);
  launch(std::string(OPERATIONS_BINARY) + " --submissions " + in + "ops.ring --acks " + in +
             "opsacks.ring --events " + in +
             "events.ring --instruments 1 --define 1:5:1:5:1000000:100000000:100000"
             " --calendar 0:CONTINUOUS --gateway-id 1",
         pids);
  REQUIRE(appears(room / "gw.ring"));
  REQUIRE(appears(room / "ops.ring"));
  launch(std::string(SEQUENCER_BINARY) + " --in " + in + "gw.ring," + in + "ops.ring --acks " + in +
             "gwacks.ring," + in + "opsacks.ring --out " + in + "seq.ring --journal " + in +
             "seq.exj",
         pids);
  REQUIRE(appears(room / "seq.ring"));
  launch(std::string(MATCHER_BINARY) + " --in " + in + "seq.ring --out " + in +
             "events.ring --journal " + in + "matcher.exj",
         pids);
  REQUIRE(appears(room / "events.ring"));
  launch(std::string(MARKETDATA_BINARY) + " --in " + in +
             "events.ring --a 127.0.0.1:36102 --b 127.0.0.1:36103 --rewind 36104",
         pids);

  // The dealer takes the A feed and measures its own wire to wire; the crowd takes the B twin
  // and must trade, which is the whole venue answering in one exit code.
  launch(std::string(BOT_BINARY) + " --connect 36101 --participant 7 --secret 42 --feed 36102" +
             " --rewind 36104 --role maker --duration-s 12 --results " + in +
             " --label wire-to-wire",
         pids);
  const int crowd = std::system((std::string(BOT_BINARY) +
                                 " --connect 36101 --participant 8 --secret 43 --feed 36103"
                                 " --rewind 36104 --role noise --seed 9 --duration-s 6"
                                 " --expect-trades")
                                    .c_str());
  CHECK(crowd == 0);

  // The dealer's measurement landed, and the venue journaled a day that actually happened.
  CHECK(appears(room / "wire-to-wire-manifest.json"));
  CHECK(std::filesystem::file_size(room / "seq.exj") > 0);
  CHECK(std::filesystem::file_size(room / "matcher.exj") > 0);
}
