// The snapshot's byte plumbing: a sink that appends primitives and raw spans, and a source that
// reads them back in the same order. State is saved as the native little-endian words the arrays
// already hold, which is what makes a snapshot a copy rather than a translation; the envelope in
// snapshot.hpp carries the versions that make that safe to evolve.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace exchange::matcher {

class ByteSink {
 public:
  void u32(const std::uint32_t value) { raw(&value, sizeof value); }
  void u64(const std::uint64_t value) { raw(&value, sizeof value); }
  void i32(const std::int32_t value) { raw(&value, sizeof value); }
  void i64(const std::int64_t value) { raw(&value, sizeof value); }

  void raw(const void* data, const std::size_t length) {
    const char* bytes = static_cast<const char*>(data);
    bytes_.insert(bytes_.end(), bytes, bytes + length);
  }

  template <typename Word>
  void span(const std::vector<Word>& words) {
    u64(words.size());
    raw(words.data(), words.size() * sizeof(Word));
  }

  const std::vector<char>& bytes() const { return bytes_; }

 private:
  std::vector<char> bytes_;
};

class ByteSource {
 public:
  ByteSource(const char* data, const std::size_t length) : at_(data), end_(data + length) {}

  std::uint32_t u32() { return read<std::uint32_t>(); }
  std::uint64_t u64() { return read<std::uint64_t>(); }
  std::int32_t i32() { return read<std::int32_t>(); }
  std::int64_t i64() { return read<std::int64_t>(); }

  void raw(void* data, const std::size_t length) {
    if (at_ + length > end_) {
      throw std::runtime_error("the snapshot ends before its state does");
    }
    std::memcpy(data, at_, length);
    at_ += length;
  }

  template <typename Word>
  void span(std::vector<Word>& words) {
    words.resize(u64());
    raw(words.data(), words.size() * sizeof(Word));
  }

  bool exhausted() const { return at_ == end_; }

 private:
  template <typename Value>
  Value read() {
    Value value;
    raw(&value, sizeof value);
    return value;
  }

  const char* at_;
  const char* end_;
};

}  // namespace exchange::matcher
