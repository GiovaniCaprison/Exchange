// The append-only record of a sequenced stream, in docs/PROTOCOL.md's format: a header naming the
// schema, then length-prefixed framed messages each followed by a CRC32C and padded to eight
// bytes. The writer appends and flushes per record; the reader consumes records until the file
// ends or a record is torn, truncating the torn tail, and verifies that command sequences are
// contiguous. A torn tail is the expected result of a process dying mid-append and is repaired by
// truncation rather than guesswork.

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "crc32c.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/NewOrder.h"

namespace exchange::common::journal {

inline constexpr char MAGIC[8] = {'E', 'X', 'J', 'R', 'N', 'L', '0', '1'};

inline std::size_t padded(const std::size_t length) { return (length + 7) & ~std::size_t{7}; }

class Writer {
 public:
  // Appending continues an existing journal past its sound bytes, which is how a standby's file
  // becomes the new primary's without a copy; a fresh path gets the header either way.
  explicit Writer(const std::string& path, const bool append = false) {
    const bool exists =
        append && std::filesystem::exists(path) && std::filesystem::file_size(path) >= sizeof MAGIC;
    file_ = std::fopen(path.c_str(), exists ? "ab" : "wb");
    if (file_ == nullptr) {
      throw std::runtime_error("cannot open journal " + path);
    }
    if (!exists) {
      std::fwrite(MAGIC, 1, sizeof MAGIC, file_);
      const std::uint32_t schemaId = ::exchange::protocol::NewOrder::sbeSchemaId();
      const std::uint32_t schemaVersion = ::exchange::protocol::NewOrder::sbeSchemaVersion();
      std::fwrite(&schemaId, sizeof schemaId, 1, file_);
      std::fwrite(&schemaVersion, sizeof schemaVersion, 1, file_);
    }
  }

  ~Writer() { close(); }

  void append(const char* message, const std::uint32_t length) {
    std::fwrite(&length, sizeof length, 1, file_);
    std::fwrite(message, 1, length, file_);
    const std::uint32_t crc = crc32c(message, length);
    std::fwrite(&crc, sizeof crc, 1, file_);
    const std::size_t written = sizeof length + length + sizeof crc;
    const char zeros[8] = {};
    std::fwrite(zeros, 1, padded(written) - written, file_);
    std::fflush(file_);
  }

  void close() {
    if (file_ != nullptr) {
      std::fclose(file_);
      file_ = nullptr;
    }
  }

 private:
  std::FILE* file_ = nullptr;
};

struct Read {
  // Every whole, checksummed record's message bytes, concatenated, with offsets and lengths.
  std::vector<char> messages;
  std::vector<std::size_t> offsets;
  std::vector<std::size_t> lengths;
  // How many bytes of the file were sound; anything beyond is the torn tail a rewrite truncates.
  std::size_t soundBytes = 0;

  std::size_t count() const { return offsets.size(); }
};

inline Read read(const std::string& path) {
  std::FILE* file = std::fopen(path.c_str(), "rb");
  if (file == nullptr) {
    throw std::runtime_error("cannot open journal " + path);
  }
  std::fseek(file, 0, SEEK_END);
  const long size = std::ftell(file);
  std::fseek(file, 0, SEEK_SET);
  std::vector<char> bytes(static_cast<std::size_t>(size));
  const std::size_t got = std::fread(bytes.data(), 1, bytes.size(), file);
  std::fclose(file);
  bytes.resize(got);

  Read result;
  const std::size_t header = sizeof MAGIC + 2 * sizeof(std::uint32_t);
  if (bytes.size() < header || std::memcmp(bytes.data(), MAGIC, sizeof MAGIC) != 0) {
    throw std::runtime_error(path + " is not a journal");
  }
  std::size_t at = header;
  result.soundBytes = at;
  while (at + sizeof(std::uint32_t) <= bytes.size()) {
    std::uint32_t length = 0;
    std::memcpy(&length, bytes.data() + at, sizeof length);
    const std::size_t whole = padded(sizeof length + length + sizeof(std::uint32_t));
    if (length == 0 || at + sizeof length + length + sizeof(std::uint32_t) > bytes.size()) {
      break;
    }
    std::uint32_t crc = 0;
    std::memcpy(&crc, bytes.data() + at + sizeof length + length, sizeof crc);
    if (crc != crc32c(bytes.data() + at + sizeof length, length)) {
      break;
    }
    result.offsets.push_back(result.messages.size());
    result.lengths.push_back(length);
    result.messages.insert(result.messages.end(), bytes.begin() + static_cast<long>(at + 4),
                           bytes.begin() + static_cast<long>(at + 4 + length));
    at += whole;
    result.soundBytes = at;
  }
  return result;
}

}  // namespace exchange::common::journal
