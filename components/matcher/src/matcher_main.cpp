// The matcher process. Two ways in, one engine: a live mode consuming a shared-memory ring and
// journaling every command before applying it, and a replay mode consuming a journal directly,
// which is also what recovery is. Because the engine is a deterministic function of the sequenced
// stream (P-2), the replay mode is not a test fixture bolted on: it is the recovery mechanism,
// the failover rehearsal and the audit trail, all the same binary flag.
//
//   matcher --journal J --events OUT [--restore SNAP] [--snapshot SNAP --snapshot-at N]
//           [--stop-at N]
//   matcher --in RING --out RING --journal J [--restore SNAP] [--snapshot SNAP]
//
// In replay mode, events go to a file for diffing; --restore starts from a snapshot and replays
// the journal suffix; --snapshot-at writes the snapshot after applying that sequence; --stop-at
// stops after applying that sequence, which is how a determinism test kills a run at a chosen
// point without a race. In live mode, SIGINT or SIGTERM stops the loop and writes the snapshot if
// a path was given.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "broadcast_ring.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/RewindRequest.h"
#include "journal.hpp"
#include "partition.hpp"
#include "snapshot.hpp"
#include "spsc_ring.hpp"
#include "stream.hpp"

namespace {

namespace matching = exchange::matcher;
namespace common = exchange::common;

volatile std::sig_atomic_t stopped = 0;

void onSignal(int) { stopped = 1; }

// Replay mode's ring: claims from a scratch buffer and keeps every committed byte for the file.
class FileRing {
 public:
  std::size_t claim(const std::size_t length) {
    const std::size_t aligned = (length + 7) & ~std::size_t{7};
    if (cursor_ + aligned > space_.size()) {
      cursor_ = 0;
    }
    claimed_ = cursor_;
    claimedLength_ = length;
    cursor_ += aligned;
    return claimed_;
  }

  char* buffer() { return space_.data(); }

  void commit() {
    captured_.insert(captured_.end(), space_.begin() + static_cast<long>(claimed_),
                     space_.begin() + static_cast<long>(claimed_ + claimedLength_));
  }

  void publish() {}

  const std::vector<char>& captured() const { return captured_; }

 private:
  std::vector<char> space_ = std::vector<char>(1 << 20);
  std::vector<char> captured_;
  std::size_t cursor_ = 0;
  std::size_t claimed_ = 0;
  std::size_t claimedLength_ = 0;
};

std::string argument(const int count, char** values, const std::string& name) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return "";
}

std::uint64_t sequenceOf(char* message, const std::size_t length) {
  namespace sbe = exchange::protocol;
  sbe::MessageHeader header;
  header.wrap(message, 0, 0, length);
  // Every command opens with the same context, so the sequence sits at the body's start.
  std::uint64_t sequence = 0;
  std::memcpy(&sequence, message + sbe::MessageHeader::encodedLength(), sizeof sequence);
  return sequence;
}

int replay(const std::string& journalPath, const std::string& eventsPath,
           const std::string& restorePath, const std::string& snapshotPath,
           const std::uint64_t snapshotAt, const std::uint64_t stopAt) {
  FileRing ring;
  matching::Partition<FileRing> partition(ring);
  std::uint64_t from = 0;
  if (!restorePath.empty()) {
    from = common::snapshot::restore(restorePath, partition);
  }
  common::journal::Read log = common::journal::read(journalPath);
  for (std::size_t at = 0; at < log.count(); at++) {
    char* message = log.messages.data() + log.offsets[at];
    const std::uint64_t sequence = sequenceOf(message, log.lengths[at]);
    if (sequence <= from) {
      continue;
    }
    partition.onCommand(message, 0, log.lengths[at]);
    if (!snapshotPath.empty() && sequence == snapshotAt) {
      common::snapshot::write(snapshotPath, partition);
    }
    if (stopAt != 0 && sequence == stopAt) {
      break;
    }
  }
  std::FILE* out = std::fopen(eventsPath.c_str(), "wb");
  if (out == nullptr) {
    std::fprintf(stderr, "cannot write events %s\n", eventsPath.c_str());
    return 2;
  }
  std::fwrite(ring.captured().data(), 1, ring.captured().size(), out);
  std::fclose(out);
  return 0;
}

int live(const std::string& inPath, const std::string& outPath, const std::string& journalPath,
         const std::string& restorePath, const std::string& snapshotPath,
         const std::vector<std::uint32_t>& instruments, const std::uint32_t shard) {
  // Events broadcast: the gateway, the market data publisher and the operations scheduler all
  // read this ring, each on its own seat, none of them able to stall the matcher.
  common::BroadcastRing out = common::BroadcastRing::create(outPath, 1 << 24);
  common::SpscRing in = common::SpscRing::attach(inPath);
  matching::Partition<common::BroadcastRing> partition(out);
  if (!instruments.empty()) {
    partition.serve(instruments);
  }
  if (shard != 0) {
    partition.shard(shard);
  }
  std::uint64_t from = 0;
  if (!restorePath.empty()) {
    from = common::snapshot::restore(restorePath, partition);
  }
  common::journal::Writer journal(journalPath);
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0) {
    in.poll([&](char* message, const std::size_t length) {
      if (sequenceOf(message, length) <= from) {
        return;
      }
      // Another shard's commands never enter this shard's journal, so a shard's journal replays
      // through the same partition with no filter at all.
      std::uint32_t addressed = 0;
      std::memcpy(&addressed, message + exchange::protocol::MessageHeader::encodedLength() + 16,
                  sizeof addressed);
      if (addressed != 0 && !partition.serves(addressed)) {
        return;
      }
      journal.append(message, static_cast<std::uint32_t>(length));
      partition.onCommand(message, 0, length);
    });
  }
  if (!snapshotPath.empty()) {
    common::snapshot::write(snapshotPath, partition);
  }
  return 0;
}

// The warm twin: the same sequenced commands, heard over the packet feed instead of the ring,
// repaired through the rewinder when the wire loses one, matched by the same deterministic
// engine into its own broadcast ring and journal. Because the engine is a pure function of the
// stream, the twin's events are the primary's, byte for byte, which is why a consumer seated at
// both rings hears one stream and the primary's death is a non-event.
int follow(const std::uint16_t feedPort, const std::string& repairHost,
           const std::uint16_t repairPort, const std::string& outPath,
           const std::string& journalPath, const std::string& restorePath,
           const std::string& snapshotPath, const std::vector<std::uint32_t>& instruments,
           const std::uint32_t shard) {
  const int wire = ::socket(AF_INET, SOCK_DGRAM, 0);
  {
    const int reuse = 1;
    ::setsockopt(wire, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof reuse);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(feedPort);
    if (::bind(wire, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0) {
      std::fprintf(stderr, "cannot bind the feed port %u\n", feedPort);
      return 2;
    }
    ::fcntl(wire, F_SETFL, ::fcntl(wire, F_GETFL, 0) | O_NONBLOCK);
  }
  // Asks leave from the bound feed socket, so the rewinder's answers come back through the one
  // door every packet uses.
  struct Ask {
    int wire = -1;
    sockaddr_in to{};
    void request(const std::uint64_t firstSequence, const std::uint32_t count) {
      char space[64] = {};
      exchange::protocol::RewindRequest encoder;
      encoder.wrapAndApplyHeader(space, 0, sizeof space);
      encoder.firstSequence(firstSequence).count(count).reserved(0);
      ::sendto(wire, space,
               exchange::protocol::MessageHeader::encodedLength() +
                   exchange::protocol::RewindRequest::sbeBlockLength(),
               0, reinterpret_cast<const sockaddr*>(&to), sizeof to);
    }
  } ask;
  ask.wire = wire;
  ask.to.sin_family = AF_INET;
  ask.to.sin_port = htons(repairPort);
  ::inet_pton(AF_INET, repairHost.c_str(), &ask.to.sin_addr);

  common::BroadcastRing out = common::BroadcastRing::create(outPath, 1 << 24);
  matching::Partition<common::BroadcastRing> partition(out);
  if (!instruments.empty()) {
    partition.serve(instruments);
  }
  if (shard != 0) {
    partition.shard(shard);
  }
  std::uint64_t from = 0;
  if (!restorePath.empty()) {
    from = common::snapshot::restore(restorePath, partition);
  }
  common::journal::Writer journal(journalPath);
  common::stream::PacketSource<Ask> source(from + 1, 1, ask);
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  char datagram[2048];
  while (stopped == 0 && !source.ended()) {
    const long got = ::recv(wire, datagram, sizeof datagram, 0);
    if (got <= 0) {
      ::usleep(100);
      continue;
    }
    source.onPacket(
        datagram, static_cast<std::size_t>(got), [&](char* message, const std::size_t length) {
          std::uint32_t addressed = 0;
          std::memcpy(&addressed, message + exchange::protocol::MessageHeader::encodedLength() + 16,
                      sizeof addressed);
          if (addressed != 0 && !partition.serves(addressed)) {
            return;
          }
          journal.append(message, static_cast<std::uint32_t>(length));
          partition.onCommand(message, 0, length);
        });
  }
  if (!snapshotPath.empty()) {
    common::snapshot::write(snapshotPath, partition);
  }
  ::close(wire);
  return 0;
}

}  // namespace

int main(const int count, char** values) {
  const std::string journalPath = argument(count, values, "--journal");
  const std::string eventsPath = argument(count, values, "--events");
  const std::string inPath = argument(count, values, "--in");
  const std::string outPath = argument(count, values, "--out");
  const std::string restorePath = argument(count, values, "--restore");
  const std::string snapshotPath = argument(count, values, "--snapshot");
  const std::string snapshotAt = argument(count, values, "--snapshot-at");
  const std::string stopAt = argument(count, values, "--stop-at");
  const std::string instrumentList = argument(count, values, "--instruments");
  const std::string shard = argument(count, values, "--shard");
  std::vector<std::uint32_t> instruments;
  {
    std::size_t at = 0;
    while (at < instrumentList.size()) {
      const std::size_t comma = instrumentList.find(',', at);
      instruments.push_back(static_cast<std::uint32_t>(std::stoul(
          instrumentList.substr(at, comma == std::string::npos ? std::string::npos : comma - at))));
      if (comma == std::string::npos) {
        break;
      }
      at = comma + 1;
    }
  }

  if (!journalPath.empty() && !eventsPath.empty()) {
    return replay(journalPath, eventsPath, restorePath, snapshotPath,
                  snapshotAt.empty() ? 0 : std::stoull(snapshotAt),
                  stopAt.empty() ? 0 : std::stoull(stopAt));
  }
  const std::string followUdp = argument(count, values, "--follow-udp");
  const std::string repairUdp = argument(count, values, "--repair-udp");
  if (!followUdp.empty() && !repairUdp.empty() && !outPath.empty() && !journalPath.empty()) {
    const std::size_t colon = repairUdp.find(':');
    if (colon == std::string::npos) {
      std::fprintf(stderr, "--repair-udp wants HOST:PORT\n");
      return 2;
    }
    return follow(static_cast<std::uint16_t>(std::stoi(followUdp)), repairUdp.substr(0, colon),
                  static_cast<std::uint16_t>(std::stoi(repairUdp.substr(colon + 1))), outPath,
                  journalPath, restorePath, snapshotPath, instruments,
                  shard.empty() ? 0 : static_cast<std::uint32_t>(std::stoul(shard)));
  }
  if (!inPath.empty() && !outPath.empty() && !journalPath.empty()) {
    return live(inPath, outPath, journalPath, restorePath, snapshotPath, instruments,
                shard.empty() ? 0 : static_cast<std::uint32_t>(std::stoul(shard)));
  }
  std::fprintf(stderr,
               "usage: matcher --journal J --events OUT [--restore SNAP]\n"
               "               [--snapshot SNAP --snapshot-at N] [--stop-at N]\n"
               "       matcher --in RING --out RING --journal J [--restore SNAP]"
               " [--snapshot SNAP]\n"
               "               [--instruments 1,3,5 --shard N]\n"
               "       matcher --follow-udp PORT --repair-udp HOST:PORT --out RING --journal J\n"
               "               [--restore SNAP --snapshot SNAP --instruments ... --shard N]\n");
  return 2;
}
