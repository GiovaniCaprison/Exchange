// The packet feed is the one lossy carrier, so its consumer is proved against loss: dropped
// packets are detected by sequence, the rewinder is asked for exactly the missing range, and the
// stitched stream is byte identical to the one nothing dropped. Epochs are enforced at the same
// door: a stale range is refused outright, which is the consumer's half of leadership.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "flow.hpp"
#include "ranges.hpp"
#include "rewinder.hpp"
#include "stream.hpp"
#include "submission.hpp"

using namespace exchange::sequencer;
using namespace exchange::sequencer::test;
namespace common = exchange::common;
using exchange::matcher::test::generatedFlow;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-packets-" + name);
}

// One deterministic run whose journal and packets every case here consumes.
struct Run {
  std::vector<CommandWriter::Framed> flow;
  std::filesystem::path journalPath;
  std::vector<std::vector<char>> packets;
  std::vector<char> expected;

  Run(const std::uint64_t seed, const std::uint64_t commands, const std::string& name)
      : flow(generatedFlow(seed, commands)), journalPath(scratch(name)) {
    Wired wired(journalPath.string(), 2);
    std::vector<std::vector<char>> records = dealtSubmissions(flow, 2);
    for (std::vector<char>& record : records) {
      wired.sequencer.onSubmission(record.data(), record.size());
    }
    wired.sequencer.endSession();
    packets = wired.packets.packets;
    for (const CommandWriter::Framed& framed : flow) {
      expected.insert(expected.end(), framed.bytes.begin(), framed.bytes.end());
    }
  }

  ~Run() { std::filesystem::remove(journalPath); }
};

// Feeds packets through a source, serving every rewind request from the journal between
// packets, the way a transport carries a re-request to the rewinder and its answer back.
template <typename Source>
std::vector<char> pump(Source& source, RewindChannel& channel, Rewinder& rewinder,
                       const std::vector<std::vector<char>>& packets) {
  std::vector<char> delivered;
  const auto handler = [&](char* bytes, const std::size_t length) {
    delivered.insert(delivered.end(), bytes, bytes + length);
  };
  for (const std::vector<char>& packet : packets) {
    std::vector<char> copy = packet;
    source.onPacket(copy.data(), copy.size(), handler);
    while (!channel.requests.empty()) {
      const RewindChannel::Request request = channel.requests.front();
      channel.requests.erase(channel.requests.begin());
      rewinder.serve(
          request.firstSequence, request.count,
          [&](char* bytes, const std::size_t length) { source.onPacket(bytes, length, handler); });
    }
  }
  return delivered;
}

}  // namespace

TEST_CASE("a range builds and reads back") {
  char buffer[256];
  common::ranges::Builder builder(buffer, sizeof buffer);
  builder.open(41, 3);
  const char first[] = "hello";
  const char second[] = "sequencer";
  builder.add(first, sizeof first);
  builder.add(second, sizeof second);
  const std::size_t length = builder.close();

  common::ranges::Reader reader(buffer, length);
  CHECK(reader.firstSequence() == 41);
  CHECK(reader.epoch() == 3);
  CHECK(reader.count() == 2);
  std::vector<std::string> seen;
  reader.forEach([&](char* bytes, const std::size_t size) { seen.emplace_back(bytes, size - 1); });
  REQUIRE(seen.size() == 2);
  CHECK(seen[0] == "hello");
  CHECK(seen[1] == "sequencer");
}

TEST_CASE("a lossless packet feed delivers the stream in order") {
  Run run(21, 3000, "lossless.exj");
  RewindChannel channel;
  Rewinder rewinder(run.journalPath.string());
  common::stream::PacketSource<RewindChannel> source(1, 1, channel);
  const std::vector<char> delivered = pump(source, channel, rewinder, run.packets);
  CHECK(delivered == run.expected);
  CHECK(channel.requests.empty());
  CHECK(source.ended());
  CHECK(source.nextSequence() == run.flow.size() + 1);
}

TEST_CASE("dropped packets are rewound and the stitched stream is whole") {
  Run run(23, 3000, "dropped.exj");
  REQUIRE(run.packets.size() > 6);
  std::vector<std::vector<char>> lossy = run.packets;
  // Two separate losses: one mid-stream packet, and two adjacent ones further on.
  lossy.erase(lossy.begin() + 2);
  lossy.erase(lossy.begin() + 4, lossy.begin() + 6);

  RewindChannel channel;
  Rewinder rewinder(run.journalPath.string());
  common::stream::PacketSource<RewindChannel> source(1, 1, channel);
  const std::vector<char> delivered = pump(source, channel, rewinder, lossy);
  CHECK(delivered == run.expected);
  CHECK(source.ended());
}

TEST_CASE("a heartbeat reveals silence-hidden loss") {
  Run run(29, 200, "silent.exj");
  // Every data packet after the first is lost; only the closing heartbeat-shaped end arrives.
  std::vector<std::vector<char>> lossy;
  lossy.push_back(run.packets.front());
  lossy.push_back(run.packets.back());

  RewindChannel channel;
  Rewinder rewinder(run.journalPath.string());
  common::stream::PacketSource<RewindChannel> source(1, 1, channel);
  const std::vector<char> delivered = pump(source, channel, rewinder, lossy);
  CHECK(delivered == run.expected);
  CHECK(source.ended());
}

TEST_CASE("a stale epoch is refused and a newer one is adopted") {
  Run run(31, 60, "epochs.exj");
  RewindChannel channel;
  Rewinder rewinder(run.journalPath.string());
  common::stream::PacketSource<RewindChannel> source(1, 2, channel);

  // Everything the old leader published under epoch 1 is dead on arrival.
  std::vector<char> delivered;
  std::vector<char> copy = run.packets.front();
  source.onPacket(copy.data(), copy.size(), [&](char* bytes, const std::size_t length) {
    delivered.insert(delivered.end(), bytes, bytes + length);
  });
  CHECK(delivered.empty());
  CHECK(source.nextSequence() == 1);

  // A range from epoch 3 is a newer leadership and is adopted.
  char buffer[256];
  common::ranges::Builder builder(buffer, sizeof buffer);
  builder.open(1, 3);
  builder.add(run.flow[0].bytes.data(), static_cast<std::uint16_t>(run.flow[0].bytes.size()));
  const std::size_t length = builder.close();
  source.onPacket(buffer, length, [&](char* bytes, const std::size_t size) {
    delivered.insert(delivered.end(), bytes, bytes + size);
  });
  CHECK(source.epoch() == 3);
  CHECK(delivered == run.flow[0].bytes);
}
