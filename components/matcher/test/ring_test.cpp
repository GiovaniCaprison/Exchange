// The shared-memory ring: messages cross whole and in order, wraps are invisible to the consumer,
// nothing is visible before publish, and a whole command's batch becomes visible at once, which is
// the claim-then-publish shape the transport exists for (P-12).

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "spsc_ring.hpp"

using namespace exchange::matcher;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-ring-" + name);
}

void put(SpscRing& ring, const std::uint64_t value) {
  const std::size_t at = ring.claim(sizeof value);
  std::memcpy(ring.buffer() + at, &value, sizeof value);
  ring.commit();
}

}  // namespace

TEST_CASE("nothing is visible before publish, and a batch appears whole") {
  const std::filesystem::path path = scratch("batch.ring");
  SpscRing producer = SpscRing::create(path.string(), 1 << 12);
  SpscRing consumer = SpscRing::attach(path.string());

  put(producer, 1);
  put(producer, 2);
  std::size_t seen = 0;
  consumer.poll([&](char*, std::size_t) { seen++; });
  CHECK(seen == 0);

  producer.publish();
  std::vector<std::uint64_t> values;
  consumer.poll([&](char* bytes, std::size_t) {
    std::uint64_t value = 0;
    std::memcpy(&value, bytes, sizeof value);
    values.push_back(value);
  });
  CHECK(values == std::vector<std::uint64_t>{1, 2});
  std::filesystem::remove(path);
}

TEST_CASE("a small ring wraps invisibly and back pressure never drops a message") {
  const std::filesystem::path path = scratch("wrap.ring");
  SpscRing producer = SpscRing::create(path.string(), 1 << 10);
  SpscRing consumer = SpscRing::attach(path.string());
  constexpr std::uint64_t MESSAGES = 200'000;

  std::thread consuming([&consumer] {
    std::uint64_t expected = 0;
    while (expected < MESSAGES) {
      consumer.poll([&](char* bytes, std::size_t) {
        std::uint64_t value = 0;
        std::memcpy(&value, bytes, sizeof value);
        REQUIRE(value == expected);
        expected++;
      });
    }
  });

  for (std::uint64_t value = 0; value < MESSAGES; value++) {
    put(producer, value);
    if (value % 7 == 0) {
      producer.publish();
    }
  }
  producer.publish();
  consuming.join();
  std::filesystem::remove(path);
}
