// Determinism is the availability story (P-2), so it is proved the way it will be used: identical
// input replayed twice diffs to nothing; a snapshot taken mid-stream and restored produces a
// suffix byte identical to the run that never stopped; a journal survives a torn tail by
// truncation; and the matcher binary itself is killed at a chosen sequence, restored from its
// snapshot, and held to the uninterrupted run's bytes.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "harness.hpp"
#include "journal.hpp"
#include "partition.hpp"
#include "snapshot.hpp"

using namespace exchange::matcher;
using namespace exchange::matcher::test;

namespace {

constexpr std::uint32_t INSTRUMENT = 1;

// The same deterministic flow the invariants use, kept here as framed commands so it can feed a
// partition, a journal and the binary alike.
std::vector<CommandWriter::Framed> generatedFlow(const std::uint64_t seed,
                                                 const std::uint64_t commands) {
  CommandWriter writer;
  std::vector<CommandWriter::Framed> flow;
  flow.push_back(writer.instrument(INSTRUMENT, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  flow.push_back(writer.session(INSTRUMENT, sbe::SessionState::CONTINUOUS));
  std::uint64_t state = seed * 2685821657736338717ULL + 1;
  const auto next = [&state]() {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * 2685821657736338717ULL;
  };
  std::vector<std::uint64_t> live;
  std::uint64_t client = 0;
  for (std::uint64_t at = 0; at < commands; at++) {
    const std::uint64_t roll = next() % 100;
    if (at % 1500 == 0) {
      flow.push_back(writer.session(INSTRUMENT, at % 3000 == 0 ? sbe::SessionState::OPENING_AUCTION
                                                               : sbe::SessionState::CONTINUOUS));
      continue;
    }
    if (roll < 40 && !live.empty()) {
      const std::size_t pick = next() % live.size();
      const std::uint64_t name = live[pick];
      live[pick] = live.back();
      live.pop_back();
      flow.push_back(writer.cancel(INSTRUMENT, name, 1 + static_cast<std::uint32_t>(name % 5)));
      continue;
    }
    if (roll < 50 && !live.empty()) {
      const std::uint64_t name = live[next() % live.size()];
      flow.push_back(writer.replace(INSTRUMENT, name, 1 + static_cast<std::uint32_t>(name % 5),
                                    1 + static_cast<std::int64_t>(next() % 60),
                                    100000 + 5 * static_cast<std::int64_t>(next() % 40) -
                                        5 * static_cast<std::int64_t>(next() % 40)));
      continue;
    }
    client++;
    const bool aggressive = roll >= 88;
    const bool buy = next() % 2 == 0;
    const std::int64_t offset = 5 * static_cast<std::int64_t>(next() % (aggressive ? 3 : 40));
    const std::int64_t price =
        buy ? 100000 - (aggressive ? -offset : offset) : 100000 + (aggressive ? -offset : offset);
    const bool ioc = aggressive || next() % 20 == 0;
    const std::int64_t quantity = 1 + static_cast<std::int64_t>(next() % 40);
    const std::int64_t display =
        next() % 12 == 0 ? 1 + static_cast<std::int64_t>(next() % quantity) : 0;
    const std::int64_t trigger = next() % 40 == 0
                                     ? 100000 + 5 * static_cast<std::int64_t>(next() % 30) -
                                           5 * static_cast<std::int64_t>(next() % 30)
                                     : 0;
    if (!ioc) {
      live.push_back(client);
    }
    flow.push_back(writer.newOrder(
        INSTRUMENT, client, 1 + static_cast<std::uint32_t>(client % 5),
        buy ? sbe::Side::BUY : sbe::Side::SELL, sbe::Pricing::LIMIT,
        ioc ? sbe::TimeInForce::IMMEDIATE_OR_CANCEL : sbe::TimeInForce::GOOD_TILL_CANCEL, false,
        std::max<std::int64_t>(price, 5), quantity, 0, display,
        trigger == 0 ? 0 : std::max<std::int64_t>(trigger, 5),
        next() % 10 == 0 ? 1 + next() % 5 : 0));
  }
  return flow;
}

std::vector<char> replayAll(const std::vector<CommandWriter::Framed>& flow) {
  CapturingRing ring;
  Partition<CapturingRing> partition(ring);
  for (const CommandWriter::Framed& framed : flow) {
    std::vector<char> bytes = framed.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  }
  return ring.captured();
}

std::vector<char> fileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-determinism-" + name);
}

}  // namespace

TEST_CASE("identical input produces byte identical output") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(11, 4000);
  CHECK(replayAll(flow) == replayAll(flow));
}

TEST_CASE("a snapshot mid-stream restores to a byte identical suffix") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(20260826, 4000);
  const std::size_t cut = 2000;

  CapturingRing wholeRing;
  Partition<CapturingRing> whole(wholeRing);
  std::size_t suffixStart = 0;
  ByteSink saved;
  for (std::size_t at = 0; at < flow.size(); at++) {
    std::vector<char> bytes = flow[at].bytes;
    whole.onCommand(bytes.data(), 0, bytes.size());
    if (at + 1 == cut) {
      whole.save(saved);
      suffixStart = wholeRing.captured().size();
    }
  }

  CapturingRing restoredRing;
  Partition<CapturingRing> restored(restoredRing);
  ByteSource source(saved.bytes().data(), saved.bytes().size());
  restored.restore(source);
  CHECK(source.exhausted());
  for (std::size_t at = cut; at < flow.size(); at++) {
    std::vector<char> bytes = flow[at].bytes;
    restored.onCommand(bytes.data(), 0, bytes.size());
  }

  const std::vector<char>& original = wholeRing.captured();
  const std::vector<char> suffix(original.begin() + static_cast<long>(suffixStart), original.end());
  CHECK(restoredRing.captured() == suffix);
}

TEST_CASE("a journal replays to identical bytes and survives a torn tail") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(7, 2000);
  const std::filesystem::path path = scratch("journal.exj");
  {
    journal::Writer writer(path.string());
    for (const CommandWriter::Framed& framed : flow) {
      writer.append(framed.bytes.data(), static_cast<std::uint32_t>(framed.bytes.size()));
    }
  }
  journal::Read whole = journal::read(path.string());
  REQUIRE(whole.count() == flow.size());

  CapturingRing ring;
  Partition<CapturingRing> partition(ring);
  for (std::size_t at = 0; at < whole.count(); at++) {
    partition.onCommand(whole.messages.data() + whole.offsets[at], 0, whole.lengths[at]);
  }
  CHECK(ring.captured() == replayAll(flow));

  // A torn tail: half a record appended, as a dying process leaves one, is truncated on read.
  {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    const std::uint32_t length = 84;
    out.write(reinterpret_cast<const char*>(&length), sizeof length);
    out.write("torn", 4);
  }
  journal::Read torn = journal::read(path.string());
  CHECK(torn.count() == flow.size());
  std::filesystem::remove(path);
}

TEST_CASE("the binary is killed at a sequence, restored, and held to the unbroken run") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(31, 3000);
  const std::filesystem::path journalPath = scratch("binary.exj");
  {
    journal::Writer writer(journalPath.string());
    for (const CommandWriter::Framed& framed : flow) {
      writer.append(framed.bytes.data(), static_cast<std::uint32_t>(framed.bytes.size()));
    }
  }
  const std::filesystem::path whole = scratch("whole.bin");
  const std::filesystem::path until = scratch("until.bin");
  const std::filesystem::path after = scratch("after.bin");
  const std::filesystem::path snap = scratch("state.exs");
  const std::string binary = MATCHER_BINARY;
  const std::uint64_t snapshotAt = 1500;
  const std::uint64_t stopAt = 2100;

  REQUIRE(
      std::system((binary + " --journal " + journalPath.string() + " --events " + whole.string())
                      .c_str()) == 0);
  REQUIRE(std::system((binary + " --journal " + journalPath.string() + " --events " +
                       until.string() + " --snapshot " + snap.string() + " --snapshot-at " +
                       std::to_string(snapshotAt) + " --stop-at " + std::to_string(stopAt))
                          .c_str()) == 0);
  REQUIRE(std::system((binary + " --journal " + journalPath.string() + " --events " +
                       after.string() + " --restore " + snap.string())
                          .c_str()) == 0);

  const std::vector<char> wholeBytes = fileBytes(whole.string());
  const std::vector<char> untilBytes = fileBytes(until.string());
  const std::vector<char> afterBytes = fileBytes(after.string());

  // The dying run agreed with the unbroken one while it lived.
  REQUIRE(untilBytes.size() <= wholeBytes.size());
  CHECK(std::equal(untilBytes.begin(), untilBytes.end(), wholeBytes.begin()));

  // The restored run's whole output is the unbroken run's suffix after the snapshot point: every
  // event answering a command past snapshotAt, found by walking the raw bytes and cutting where
  // the attribution passes it.
  std::size_t cutBytes = 0;
  {
    std::vector<char> bytes = wholeBytes;
    std::size_t at = 0;
    while (at < bytes.size()) {
      sbe::MessageHeader header;
      header.wrap(bytes.data(), at, 0, bytes.size());
      std::uint64_t inputSequence = 0;
      std::memcpy(&inputSequence,
                  bytes.data() + at + sbe::MessageHeader::encodedLength() + sizeof(std::uint64_t),
                  sizeof inputSequence);
      if (inputSequence > snapshotAt) {
        break;
      }
      at += sbe::MessageHeader::encodedLength() + header.blockLength();
    }
    cutBytes = at;
  }
  const std::vector<char> expected(wholeBytes.begin() + static_cast<long>(cutBytes),
                                   wholeBytes.end());
  CHECK(afterBytes == expected);

  std::filesystem::remove(journalPath);
  std::filesystem::remove(whole);
  std::filesystem::remove(until);
  std::filesystem::remove(after);
  std::filesystem::remove(snap);
}
