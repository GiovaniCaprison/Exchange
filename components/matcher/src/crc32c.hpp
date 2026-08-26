// CRC32C (the Castagnoli polynomial), table-driven, for the journal's per-record check and the
// snapshot's trailer. A torn append is the expected way a process dies, and a checksum is what
// separates a torn tail from a corrupt middle: the first is truncated, the second is an error.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace exchange::matcher {

namespace detail {

constexpr std::array<std::uint32_t, 256> crc32cTable() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t entry = 0; entry < 256; entry++) {
    std::uint32_t remainder = entry;
    for (int bit = 0; bit < 8; bit++) {
      remainder = (remainder >> 1) ^ ((remainder & 1) ? 0x82F63B78u : 0);
    }
    table[entry] = remainder;
  }
  return table;
}

inline constexpr std::array<std::uint32_t, 256> CRC32C_TABLE = crc32cTable();

}  // namespace detail

inline std::uint32_t crc32c(const void* data, const std::size_t length) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t at = 0; at < length; at++) {
    crc = (crc >> 8) ^ detail::CRC32C_TABLE[(crc ^ bytes[at]) & 0xFF];
  }
  return crc ^ 0xFFFFFFFFu;
}

}  // namespace exchange::matcher
