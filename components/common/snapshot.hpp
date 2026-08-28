// The snapshot envelope in docs/PROTOCOL.md's format: a header naming the schema and the state
// version, the sequence the state is up to, the state blob, and a CRC32C trailer. Recovery is a
// restore plus a journal replay from upToSequence + 1, and the determinism suite proves the suffix
// byte identical to the run that never stopped (P-2).

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "bytes.hpp"
#include "crc32c.hpp"
#include "exchange_protocol/NewOrder.h"

namespace exchange::common::snapshot {

inline constexpr char MAGIC[8] = {'E', 'X', 'S', 'N', 'A', 'P', '0', '1'};
// Version 3: the slab's cold half gained the auction-only flag and the peg cap, and the engine
// state gained the peg list.
inline constexpr std::uint32_t STATE_VERSION = 3;

template <typename Partition>
void write(const std::string& path, const Partition& partition) {
  ByteSink sink;
  partition.save(sink);
  const std::vector<char>& state = sink.bytes();

  std::FILE* file = std::fopen(path.c_str(), "wb");
  if (file == nullptr) {
    throw std::runtime_error("cannot write snapshot " + path);
  }
  std::fwrite(MAGIC, 1, sizeof MAGIC, file);
  const std::uint32_t schemaId = ::exchange::protocol::NewOrder::sbeSchemaId();
  const std::uint32_t schemaVersion = ::exchange::protocol::NewOrder::sbeSchemaVersion();
  const std::uint32_t stateVersion = STATE_VERSION;
  const std::uint32_t reserved = 0;
  const std::uint64_t upToSequence = partition.upToSequence();
  const std::uint64_t stateLength = state.size();
  std::fwrite(&schemaId, sizeof schemaId, 1, file);
  std::fwrite(&schemaVersion, sizeof schemaVersion, 1, file);
  std::fwrite(&stateVersion, sizeof stateVersion, 1, file);
  std::fwrite(&reserved, sizeof reserved, 1, file);
  std::fwrite(&upToSequence, sizeof upToSequence, 1, file);
  std::fwrite(&stateLength, sizeof stateLength, 1, file);
  std::fwrite(state.data(), 1, state.size(), file);
  const std::uint32_t crc = crc32c(state.data(), state.size());
  std::fwrite(&crc, sizeof crc, 1, file);
  std::fclose(file);
}

// Restores the partition and returns the sequence the snapshot is up to: replay the journal from
// the next sequence on.
template <typename Partition>
std::uint64_t restore(const std::string& path, Partition& partition) {
  std::FILE* file = std::fopen(path.c_str(), "rb");
  if (file == nullptr) {
    throw std::runtime_error("cannot read snapshot " + path);
  }
  std::fseek(file, 0, SEEK_END);
  const long size = std::ftell(file);
  std::fseek(file, 0, SEEK_SET);
  std::vector<char> bytes(static_cast<std::size_t>(size));
  const std::size_t got = std::fread(bytes.data(), 1, bytes.size(), file);
  std::fclose(file);
  bytes.resize(got);

  const std::size_t header = sizeof MAGIC + 4 * sizeof(std::uint32_t) + 2 * sizeof(std::uint64_t);
  if (bytes.size() < header + sizeof(std::uint32_t) ||
      std::memcmp(bytes.data(), MAGIC, sizeof MAGIC) != 0) {
    throw std::runtime_error(path + " is not a snapshot");
  }
  std::uint32_t stateVersion = 0;
  std::memcpy(&stateVersion, bytes.data() + sizeof MAGIC + 8, sizeof stateVersion);
  if (stateVersion != STATE_VERSION) {
    throw std::runtime_error("snapshot state version " + std::to_string(stateVersion) +
                             " is not this matcher's " + std::to_string(STATE_VERSION));
  }
  std::uint64_t upToSequence = 0;
  std::uint64_t stateLength = 0;
  std::memcpy(&upToSequence, bytes.data() + sizeof MAGIC + 16, sizeof upToSequence);
  std::memcpy(&stateLength, bytes.data() + sizeof MAGIC + 24, sizeof stateLength);
  if (bytes.size() < header + stateLength + sizeof(std::uint32_t)) {
    throw std::runtime_error(path + " ends before its state does");
  }
  std::uint32_t crc = 0;
  std::memcpy(&crc, bytes.data() + header + stateLength, sizeof crc);
  if (crc != crc32c(bytes.data() + header, stateLength)) {
    throw std::runtime_error(path + " fails its checksum");
  }
  ByteSource source(bytes.data() + header, stateLength);
  partition.restore(source);
  return upToSequence;
}

}  // namespace exchange::common::snapshot
