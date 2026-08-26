// The behavioural corpus: fixtures of commands with their blessed events, replayed through a
// partition and diffed word for word. A fixture is the human-readable form of the contract in
// docs/PROTOCOL.md, and the renderer prints exactly the vocabulary a fixture writes, so a
// mismatch shows the two lines side by side.
//
// Fixture grammar, one line each, a line whose first word starts with '#' being a comment:
//   INSTRUMENT tick=. lot=. min=. max=. band=. open=. alloc=PRICE_TIME|PRO_RATA [inst=.]
//   SESSION <STATE> [inst=.]
//   NEW <SIDE> <PRICING> <TIF> <price> <qty> #<clientOrderId> [p=.] [min=.] [display=.]
//       [trigger=.] [smp=.] [POST_ONLY] [inst=.]
//   CANCEL #<clientOrderId> [p=.] [inst=.]
//   REPLACE #<clientOrderId> <qty> <price> [p=.] [inst=.]
//   MASS [p=.] [inst=.]
// and every other line is an expected event, in the renderer's vocabulary. Commands without
// inst= address the most recently defined instrument.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::matcher;
using namespace exchange::matcher::test;

namespace {

struct Fixture {
  std::string name;
  std::vector<std::string> lines;
};

std::filesystem::path corpusDirectory() {
  std::filesystem::path candidate = std::filesystem::current_path();
  while (true) {
    if (std::filesystem::exists(candidate / "corpus")) {
      return candidate / "corpus";
    }
    if (candidate == candidate.parent_path()) {
      throw std::runtime_error("no corpus directory above the working directory");
    }
    candidate = candidate.parent_path();
  }
}

std::vector<Fixture> corpusFixtures() {
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(corpusDirectory())) {
    if (entry.path().extension() == ".txt") {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  std::vector<Fixture> fixtures;
  for (const auto& file : files) {
    Fixture fixture;
    fixture.name = file.filename().string();
    std::ifstream in(file);
    std::string line;
    while (std::getline(in, line)) {
      // A comment is a whole line whose first word starts with '#', so #1 inside a command stays
      // an order reference.
      std::istringstream words(line);
      std::string first;
      if (words >> first && first[0] != '#') {
        fixture.lines.push_back(line);
      }
    }
    fixtures.push_back(fixture);
  }
  return fixtures;
}

std::vector<std::string> tokens(const std::string& line) {
  std::istringstream in(line);
  std::vector<std::string> out;
  std::string word;
  while (in >> word) {
    out.push_back(word);
  }
  return out;
}

std::string normalised(const std::string& line) {
  const std::vector<std::string> words = tokens(line);
  std::string out;
  for (const std::string& word : words) {
    if (!out.empty()) {
      out += " ";
    }
    out += word;
  }
  return out;
}

std::int64_t value(const std::vector<std::string>& words, const std::string& key,
                   const std::int64_t fallback) {
  for (const std::string& word : words) {
    if (word.rfind(key + "=", 0) == 0) {
      return std::stoll(word.substr(key.size() + 1));
    }
  }
  return fallback;
}

bool present(const std::vector<std::string>& words, const std::string& flag) {
  for (const std::string& word : words) {
    if (word == flag) {
      return true;
    }
  }
  return false;
}

std::uint64_t reference(const std::vector<std::string>& words) {
  for (const std::string& word : words) {
    if (word.size() > 1 && word[0] == '#') {
      return std::stoull(word.substr(1));
    }
  }
  throw std::runtime_error("a command that names an order needs a #reference");
}

sbe::Side::Value sideOf(const std::string& word) {
  return word == "BUY" ? sbe::Side::BUY : sbe::Side::SELL;
}

sbe::Pricing::Value pricingOf(const std::string& word) {
  return word == "MARKET" ? sbe::Pricing::MARKET : sbe::Pricing::LIMIT;
}

sbe::TimeInForce::Value timeInForceOf(const std::string& word) {
  if (word == "DAY") {
    return sbe::TimeInForce::DAY;
  }
  if (word == "IOC") {
    return sbe::TimeInForce::IMMEDIATE_OR_CANCEL;
  }
  if (word == "FOK") {
    return sbe::TimeInForce::FILL_OR_KILL;
  }
  return sbe::TimeInForce::GOOD_TILL_CANCEL;
}

sbe::SessionState::Value stateOf(const std::string& word) {
  if (word == "OPENING_AUCTION") {
    return sbe::SessionState::OPENING_AUCTION;
  }
  if (word == "CONTINUOUS") {
    return sbe::SessionState::CONTINUOUS;
  }
  if (word == "CLOSING_AUCTION") {
    return sbe::SessionState::CLOSING_AUCTION;
  }
  if (word == "HALTED") {
    return sbe::SessionState::HALTED;
  }
  if (word == "CLOSED") {
    return sbe::SessionState::CLOSED;
  }
  return sbe::SessionState::PRE_OPEN;
}

bool isCommand(const std::string& first) {
  return first == "INSTRUMENT" || first == "SESSION" || first == "NEW" || first == "CANCEL" ||
         first == "REPLACE" || first == "MASS";
}

struct Run {
  std::vector<std::string> expected;
  std::vector<std::string> actual;
};

Run run(const Fixture& fixture) {
  CapturingRing ring;
  Partition<CapturingRing> partition(ring);
  CommandWriter writer;
  std::uint32_t instrument = 1;
  Run result;
  for (const std::string& line : fixture.lines) {
    const std::vector<std::string> words = tokens(line);
    const std::string& first = words[0];
    if (!isCommand(first)) {
      result.expected.push_back(normalised(line));
      continue;
    }
    CommandWriter::Framed framed;
    if (first == "INSTRUMENT") {
      instrument = static_cast<std::uint32_t>(value(words, "inst", instrument));
      framed = writer.instrument(instrument, value(words, "tick", 1), value(words, "lot", 1),
                                 value(words, "min", 1), value(words, "max", 1'000'000),
                                 value(words, "band", 1'000'000), value(words, "open", 100'000),
                                 present(words, "alloc=PRO_RATA"));
    } else if (first == "SESSION") {
      framed = writer.session(static_cast<std::uint32_t>(value(words, "inst", instrument)),
                              stateOf(words[1]));
    } else if (first == "NEW") {
      framed = writer.newOrder(
          static_cast<std::uint32_t>(value(words, "inst", instrument)), reference(words),
          static_cast<std::uint32_t>(value(words, "p", 1)), sideOf(words[1]), pricingOf(words[2]),
          timeInForceOf(words[3]), present(words, "POST_ONLY"), std::stoll(words[4]),
          std::stoll(words[5]), value(words, "min", 0), value(words, "display", 0),
          value(words, "trigger", 0), static_cast<std::uint64_t>(value(words, "smp", 0)));
    } else if (first == "CANCEL") {
      framed = writer.cancel(static_cast<std::uint32_t>(value(words, "inst", instrument)),
                             reference(words), static_cast<std::uint32_t>(value(words, "p", 1)));
    } else if (first == "REPLACE") {
      framed = writer.replace(static_cast<std::uint32_t>(value(words, "inst", instrument)),
                              reference(words), static_cast<std::uint32_t>(value(words, "p", 1)),
                              std::stoll(words[2]), std::stoll(words[3]));
    } else {
      framed = writer.massCancel(static_cast<std::uint32_t>(value(words, "inst", instrument)), 0,
                                 static_cast<std::uint32_t>(value(words, "p", 1)));
    }
    partition.onCommand(framed.bytes.data(), 0, framed.bytes.size());
  }
  for (const EventView& event : readEvents(ring.captured())) {
    result.actual.push_back(render(event));
  }
  return result;
}

}  // namespace

TEST_CASE("every fixture in the corpus produces exactly its blessed events") {
  const std::vector<Fixture> fixtures = corpusFixtures();
  REQUIRE(fixtures.size() >= 25);
  for (const Fixture& fixture : fixtures) {
    const Run result = run(fixture);
    INFO("fixture " << fixture.name);
    {
      std::string expectedText;
      std::string actualText;
      for (const auto& line : result.expected) {
        expectedText += line + "\n";
      }
      for (const auto& line : result.actual) {
        actualText += line + "\n";
      }
      INFO("expected:\n" << expectedText << "actual:\n" << actualText);
      CHECK(result.actual == result.expected);
    }
  }
}
