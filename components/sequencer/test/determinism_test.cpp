// The sequencer's determinism, proved on the binaries: the same submissions sequenced twice
// produce identical journal, packet and acknowledgment files; the journal the sequencer writes
// is byte identical to the stream flowgen stamps by hand from the same seed; and the matcher
// replays the sequencer's journal to the same events as the hand-stamped one, which is the whole
// pipeline exercised end to end on the scripted clock.

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {

std::vector<char> fileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-seq-determinism-" + name);
}

int run(const std::string& command) { return std::system(command.c_str()); }

}  // namespace

TEST_CASE("the binary sequences the same submissions to identical bytes, and the matcher agrees") {
  const std::string commands = "20000";
  const std::string seed = "7";
  const std::filesystem::path submissions = scratch("subs.exj");
  const std::filesystem::path handStamped = scratch("hand.exj");

  REQUIRE(run(std::string(SUBGEN_BINARY) + " --submissions " + submissions.string() +
              " --gateways 3 --commands " + commands + " --seed " + seed) == 0);
  REQUIRE(run(std::string(FLOWGEN_BINARY) + " --journal " + handStamped.string() + " --commands " +
              commands + " --seed " + seed) == 0);

  const auto sequence = [&](const std::string& tag, const std::string& policy) {
    const std::filesystem::path journal = scratch(tag + ".exj");
    const std::filesystem::path packets = scratch(tag + ".pkt");
    const std::filesystem::path acks = scratch(tag + ".ack");
    const std::filesystem::path standby = scratch(tag + ".standby.exj");
    REQUIRE(run(std::string(SEQUENCER_BINARY) + " --submissions " + submissions.string() +
                " --journal " + journal.string() + " --packets " + packets.string() + " --acks " +
                acks.string() + " --policy " + policy + " --standby-journal " + standby.string() +
                " --end-session") == 0);
    std::vector<std::filesystem::path> artifacts{journal, packets, acks};
    if (policy == "safe") {
      artifacts.push_back(standby);
    }
    return artifacts;
  };

  const std::vector<std::filesystem::path> first = sequence("one", "local");
  const std::vector<std::filesystem::path> second = sequence("two", "local");

  // Twice through the sequencer diffs to nothing, on every artifact it writes.
  for (std::size_t at = 0; at < first.size(); at++) {
    const std::vector<char> a = fileBytes(first[at].string());
    CHECK(!a.empty());
    CHECK(a == fileBytes(second[at].string()));
  }

  // The sequencer's journal is the hand-stamped stream, byte for byte.
  CHECK(fileBytes(first[0].string()) == fileBytes(handStamped.string()));

  // The safe policy sequences the same bytes, twice, and its standby holds the same journal:
  // the durability policy changes when the world hears a command, and nothing about what it is.
  const std::vector<std::filesystem::path> safeOne = sequence("safe-one", "safe");
  const std::vector<std::filesystem::path> safeTwo = sequence("safe-two", "safe");
  for (std::size_t at = 0; at < safeOne.size(); at++) {
    CHECK(fileBytes(safeOne[at].string()) == fileBytes(safeTwo[at].string()));
  }
  CHECK(fileBytes(safeOne[0].string()) == fileBytes(handStamped.string()));
  CHECK(fileBytes(safeOne[3].string()) == fileBytes(safeOne[0].string()));
  for (const std::filesystem::path& path : safeOne) {
    std::filesystem::remove(path);
  }
  for (const std::filesystem::path& path : safeTwo) {
    std::filesystem::remove(path);
  }

  // And the matcher, replaying both journals, emits identical events: the pipeline agrees.
  const std::filesystem::path eventsSequenced = scratch("sequenced.events");
  const std::filesystem::path eventsHand = scratch("hand.events");
  REQUIRE(run(std::string(MATCHER_BINARY) + " --journal " + first[0].string() + " --events " +
              eventsSequenced.string()) == 0);
  REQUIRE(run(std::string(MATCHER_BINARY) + " --journal " + handStamped.string() + " --events " +
              eventsHand.string()) == 0);
  const std::vector<char> events = fileBytes(eventsSequenced.string());
  CHECK(!events.empty());
  CHECK(events == fileBytes(eventsHand.string()));

  for (const std::filesystem::path& path : first) {
    std::filesystem::remove(path);
  }
  for (const std::filesystem::path& path : second) {
    std::filesystem::remove(path);
  }
  std::filesystem::remove(submissions);
  std::filesystem::remove(handStamped);
  std::filesystem::remove(eventsSequenced);
  std::filesystem::remove(eventsHand);
}
