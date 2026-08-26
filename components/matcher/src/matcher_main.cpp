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

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "partition.hpp"
#include "snapshot.hpp"
#include "spsc_ring.hpp"

namespace {

namespace matching = exchange::matcher;

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
    from = matching::snapshot::restore(restorePath, partition);
  }
  matching::journal::Read log = matching::journal::read(journalPath);
  for (std::size_t at = 0; at < log.count(); at++) {
    char* message = log.messages.data() + log.offsets[at];
    const std::uint64_t sequence = sequenceOf(message, log.lengths[at]);
    if (sequence <= from) {
      continue;
    }
    partition.onCommand(message, 0, log.lengths[at]);
    if (!snapshotPath.empty() && sequence == snapshotAt) {
      matching::snapshot::write(snapshotPath, partition);
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
         const std::string& restorePath, const std::string& snapshotPath) {
  matching::SpscRing out = matching::SpscRing::create(outPath, 1 << 24);
  matching::SpscRing in = matching::SpscRing::attach(inPath);
  matching::Partition<matching::SpscRing> partition(out);
  std::uint64_t from = 0;
  if (!restorePath.empty()) {
    from = matching::snapshot::restore(restorePath, partition);
  }
  matching::journal::Writer journal(journalPath);
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0) {
    in.poll([&](char* message, const std::size_t length) {
      if (sequenceOf(message, length) <= from) {
        return;
      }
      journal.append(message, static_cast<std::uint32_t>(length));
      partition.onCommand(message, 0, length);
    });
  }
  if (!snapshotPath.empty()) {
    matching::snapshot::write(snapshotPath, partition);
  }
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

  if (!journalPath.empty() && !eventsPath.empty()) {
    return replay(journalPath, eventsPath, restorePath, snapshotPath,
                  snapshotAt.empty() ? 0 : std::stoull(snapshotAt),
                  stopAt.empty() ? 0 : std::stoull(stopAt));
  }
  if (!inPath.empty() && !outPath.empty() && !journalPath.empty()) {
    return live(inPath, outPath, journalPath, restorePath, snapshotPath);
  }
  std::fprintf(stderr,
               "usage: matcher --journal J --events OUT [--restore SNAP]\n"
               "               [--snapshot SNAP --snapshot-at N] [--stop-at N]\n"
               "       matcher --in RING --out RING --journal J [--restore SNAP]"
               " [--snapshot SNAP]\n");
  return 2;
}
