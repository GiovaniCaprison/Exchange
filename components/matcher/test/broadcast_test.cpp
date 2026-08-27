// The broadcast ring: many readers each see every message whole and in order, a reader that
// arrives late replays what the buffer still retains, the producer never waits, and a reader
// that falls a whole buffer behind knows it was lapped rather than reading torn bytes. The lap
// is a counted fact and the journal is the repair, which is the recovery posture everywhere.

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "broadcast_ring.hpp"

namespace common = exchange::common;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-broadcast-" + name);
}

void put(common::BroadcastRing& ring, const std::uint64_t value) {
  const std::size_t at = ring.claim(sizeof value);
  std::memcpy(ring.buffer() + at, &value, sizeof value);
  ring.commit();
}

std::vector<std::uint64_t> drain(common::BroadcastReader& reader) {
  std::vector<std::uint64_t> values;
  reader.poll([&](char* bytes, std::size_t) {
    std::uint64_t value = 0;
    std::memcpy(&value, bytes, sizeof value);
    values.push_back(value);
  });
  return values;
}

}  // namespace

TEST_CASE("every reader sees every message, whole and in order") {
  const std::filesystem::path path = scratch("fanout.ring");
  common::BroadcastRing producer = common::BroadcastRing::create(path.string(), 1 << 12);
  common::BroadcastReader one = common::BroadcastReader::attach(path.string());
  common::BroadcastReader two = common::BroadcastReader::attach(path.string());

  put(producer, 1);
  put(producer, 2);
  CHECK(drain(one).empty());
  producer.publish();
  put(producer, 3);
  producer.publish();

  const std::vector<std::uint64_t> everything{1, 2, 3};
  CHECK(drain(one) == everything);
  CHECK(drain(two) == everything);
  CHECK(one.laps() == 0);
  CHECK(two.laps() == 0);
  std::filesystem::remove(path);
}

TEST_CASE("a late joiner replays what the buffer retains") {
  const std::filesystem::path path = scratch("join.ring");
  common::BroadcastRing producer = common::BroadcastRing::create(path.string(), 1 << 12);
  put(producer, 7);
  put(producer, 8);
  producer.publish();

  common::BroadcastReader reader = common::BroadcastReader::attach(path.string());
  CHECK(drain(reader) == std::vector<std::uint64_t>{7, 8});
  CHECK(reader.laps() == 0);
  std::filesystem::remove(path);
}

TEST_CASE("the producer never waits, and the reader it lapped knows") {
  const std::filesystem::path path = scratch("lap.ring");
  common::BroadcastRing producer = common::BroadcastRing::create(path.string(), 1 << 10);
  common::BroadcastReader reader = common::BroadcastReader::attach(path.string());

  // Many times the buffer, with nobody reading: a back-pressured ring would hang right here.
  for (std::uint64_t value = 0; value < 1'000; value++) {
    put(producer, value);
    producer.publish();
  }

  // The reader is honest about the loss, resumes at the head, and stays whole afterwards.
  const std::vector<std::uint64_t> caught = drain(reader);
  CHECK(reader.laps() == 1);
  for (std::size_t at = 1; at < caught.size(); at++) {
    CHECK(caught[at] == caught[at - 1] + 1);
  }
  CHECK((caught.empty() ? 999 : caught.back()) == 999);

  put(producer, 1'000);
  producer.publish();
  CHECK(drain(reader) == std::vector<std::uint64_t>{1'000});
  CHECK(reader.laps() == 1);
  std::filesystem::remove(path);
}

TEST_CASE("a keeping-up reader crosses wraps intact while the producer floods") {
  const std::filesystem::path path = scratch("stress.ring");
  common::BroadcastRing producer = common::BroadcastRing::create(path.string(), 1 << 14);
  common::BroadcastReader reader = common::BroadcastReader::attach(path.string());
  constexpr std::uint64_t MESSAGES = 200'000;

  // The reader validates the whole story: values only ever step forward, and a jump is only
  // legal when a lap was counted at that moment, so a torn read has nowhere to hide. One last
  // poll after the producer finishes drains whatever is reachable; ending short of the total is
  // only legal if a lap swallowed the tail.
  std::atomic<bool> finished{false};
  std::uint64_t delivered = 0;
  std::thread consuming([&] {
    std::uint64_t expected = 0;
    std::uint64_t lapsSeen = 0;
    while (expected < MESSAGES) {
      const bool last = finished.load(std::memory_order_acquire);
      reader.poll([&](char* bytes, std::size_t) {
        std::uint64_t value = 0;
        std::memcpy(&value, bytes, sizeof value);
        if (value != expected) {
          REQUIRE(value > expected);
          REQUIRE(reader.laps() > lapsSeen);
        }
        lapsSeen = reader.laps();
        expected = value + 1;
        delivered++;
      });
      if (last) {
        break;
      }
    }
    REQUIRE((expected == MESSAGES || reader.laps() > 0));
  });

  for (std::uint64_t value = 0; value < MESSAGES; value++) {
    put(producer, value);
    if (value % 5 == 0) {
      producer.publish();
    }
  }
  producer.publish();
  finished.store(true, std::memory_order_release);
  consuming.join();
  CHECK(delivered > 0);
  std::filesystem::remove(path);
}
