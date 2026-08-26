// The sequencer process. Two ways in, one core: a live mode arbitrating gateway rings on the
// wall clock, and an offline mode consuming a submission journal on the scripted clock, which is
// how the determinism suite holds two runs to identical bytes and how a sequenced stream is
// rebuilt for audit. The offline arbitration is the file's order, because a recorded arrival
// order is exactly what the file is.
//
//   sequencer --submissions FILE --journal J [--packets P] [--acks A] [--end-session]
//   sequencer --in R1,R2,... --acks A1,A2,... --out RING --journal J [--udp HOST:PORT]
//
// In live mode the sequencer visits the gateway rings round robin, one record per visit, so no
// gateway's burst starves another; a sweep that sequenced something flushes one packet, and a
// quiet wire gets a heartbeat every hundred milliseconds. SIGINT or SIGTERM closes the session.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "clock.hpp"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "sequencer.hpp"
#include "spsc_ring.hpp"

namespace {

namespace common = exchange::common;
namespace sequencing = exchange::sequencer;
namespace sbe = exchange::protocol;

volatile std::sig_atomic_t stopped = 0;

void onSignal(int) { stopped = 1; }

class DiscardingRing {
 public:
  std::size_t claim(const std::size_t length) {
    const std::size_t aligned = (length + 7) & ~std::size_t{7};
    if (cursor_ + aligned > sizeof space_) {
      cursor_ = 0;
    }
    const std::size_t at = cursor_;
    cursor_ += aligned;
    return at;
  }
  char* buffer() { return space_; }
  void commit() {}
  void publish() {}

 private:
  char space_[1 << 16] = {};
  std::size_t cursor_ = 0;
};

// Offline acknowledgments are captured and written to a file, one gateway after another in id
// order, so a determinism diff covers the acknowledgment stream too.
class CapturingRing {
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
  std::vector<char> space_ = std::vector<char>(1 << 16);
  std::vector<char> captured_;
  std::size_t cursor_ = 0;
  std::size_t claimed_ = 0;
  std::size_t claimedLength_ = 0;
};

// Packets to a file as length-prefixed records; /dev/null when nobody asked for them.
class FilePacketSink {
 public:
  explicit FilePacketSink(const std::string& path) {
    file_ = std::fopen(path.empty() ? "/dev/null" : path.c_str(), "wb");
  }
  ~FilePacketSink() {
    if (file_ != nullptr) {
      std::fclose(file_);
    }
  }
  void send(const char* bytes, const std::size_t length) {
    const std::uint32_t size = static_cast<std::uint32_t>(length);
    std::fwrite(&size, sizeof size, 1, file_);
    std::fwrite(bytes, 1, length, file_);
  }

 private:
  std::FILE* file_ = nullptr;
};

// Live packets over UDP when an address was given; otherwise they go nowhere, since on box the
// ring is the carrier and the packet feed is for the wire.
class UdpSink {
 public:
  UdpSink(const std::string& host, const std::uint16_t port) {
    if (host.empty()) {
      return;
    }
    socket_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    to_.sin_family = AF_INET;
    to_.sin_port = htons(port);
    ::inet_pton(AF_INET, host.c_str(), &to_.sin_addr);
  }
  ~UdpSink() {
    if (socket_ >= 0) {
      ::close(socket_);
    }
  }
  void send(const char* bytes, const std::size_t length) {
    if (socket_ >= 0) {
      ::sendto(socket_, bytes, length, 0, reinterpret_cast<const sockaddr*>(&to_), sizeof to_);
    }
  }

 private:
  int socket_ = -1;
  sockaddr_in to_{};
};

std::string argument(const int count, char** values, const std::string& name) {
  for (int at = 1; at + 1 < count; at++) {
    if (name == values[at]) {
      return values[at + 1];
    }
  }
  return "";
}

bool flagged(const int count, char** values, const std::string& name) {
  for (int at = 1; at < count; at++) {
    if (name == values[at]) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> split(const std::string& list) {
  std::vector<std::string> parts;
  std::size_t from = 0;
  while (from <= list.size()) {
    const std::size_t comma = list.find(',', from);
    if (comma == std::string::npos) {
      parts.push_back(list.substr(from));
      break;
    }
    parts.push_back(list.substr(from, comma - from));
    from = comma + 1;
  }
  return parts;
}

std::uint32_t gatewayOf(char* record, const std::size_t length) {
  sbe::MessageHeader wrap;
  wrap.wrap(record, 0, 0, length);
  sbe::GatewaySubmission submission;
  submission.wrapForDecode(record, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                           length);
  return submission.gatewayId();
}

int offline(const std::string& submissionsPath, const std::string& journalPath,
            const std::string& packetsPath, const std::string& acksPath, const bool endSession) {
  common::journal::Read submissions = common::journal::read(submissionsPath);
  std::uint32_t gateways = 1;
  for (std::size_t at = 0; at < submissions.count(); at++) {
    const std::uint32_t gateway =
        gatewayOf(submissions.messages.data() + submissions.offsets[at], submissions.lengths[at]);
    gateways = gateway + 1 > gateways ? gateway + 1 : gateways;
  }

  DiscardingRing out;
  std::vector<CapturingRing> acks(gateways);
  FilePacketSink packets(packetsPath);
  common::journal::Writer journal(journalPath);
  sequencing::ScriptedClock clock;
  sequencing::Sequencer<DiscardingRing, CapturingRing, FilePacketSink, common::journal::Writer,
                        sequencing::ScriptedClock>
      sequencer(out, acks, packets, journal, clock);

  for (std::size_t at = 0; at < submissions.count(); at++) {
    sequencer.onSubmission(submissions.messages.data() + submissions.offsets[at],
                           submissions.lengths[at]);
  }
  if (endSession) {
    sequencer.endSession();
  } else {
    sequencer.flush();
  }

  if (!acksPath.empty()) {
    std::FILE* out2 = std::fopen(acksPath.c_str(), "wb");
    if (out2 == nullptr) {
      std::fprintf(stderr, "cannot write acks %s\n", acksPath.c_str());
      return 2;
    }
    for (const CapturingRing& ring : acks) {
      std::fwrite(ring.captured().data(), 1, ring.captured().size(), out2);
    }
    std::fclose(out2);
  }
  return 0;
}

int live(const std::string& inList, const std::string& ackList, const std::string& outPath,
         const std::string& journalPath, const std::string& udp) {
  const std::vector<std::string> inPaths = split(inList);
  const std::vector<std::string> ackPaths = split(ackList);
  if (inPaths.size() != ackPaths.size()) {
    std::fprintf(stderr, "every gateway ring needs its ack ring\n");
    return 2;
  }
  common::SpscRing out = common::SpscRing::create(outPath, 1 << 24);
  std::vector<common::SpscRing> acks;
  acks.reserve(ackPaths.size());
  for (const std::string& path : ackPaths) {
    acks.push_back(common::SpscRing::create(path, 1 << 20));
  }
  std::vector<common::SpscRing> ins;
  ins.reserve(inPaths.size());
  for (const std::string& path : inPaths) {
    ins.push_back(common::SpscRing::attach(path));
  }

  const std::size_t colon = udp.find(':');
  UdpSink packets(colon == std::string::npos ? "" : udp.substr(0, colon),
                  colon == std::string::npos
                      ? 0
                      : static_cast<std::uint16_t>(std::stoi(udp.substr(colon + 1))));
  common::journal::Writer journal(journalPath);
  sequencing::WallClock clock;
  sequencing::Sequencer<common::SpscRing, common::SpscRing, UdpSink, common::journal::Writer,
                        sequencing::WallClock>
      sequencer(out, acks, packets, journal, clock);

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::uint64_t lastSend = clock.now();
  while (stopped == 0) {
    std::size_t sequenced = 0;
    for (common::SpscRing& in : ins) {
      sequenced += in.pollOne(
          [&](char* record, const std::size_t length) { sequencer.onSubmission(record, length); });
    }
    if (sequenced > 0) {
      sequencer.flush();
      lastSend = clock.now();
      continue;
    }
    if (clock.now() - lastSend > 100'000'000ULL) {
      sequencer.heartbeat();
      lastSend = clock.now();
    }
  }
  sequencer.endSession();
  return 0;
}

}  // namespace

int main(const int count, char** values) {
  const std::string submissions = argument(count, values, "--submissions");
  const std::string journalPath = argument(count, values, "--journal");
  const std::string packets = argument(count, values, "--packets");
  const std::string acksPath = argument(count, values, "--acks");
  const std::string inList = argument(count, values, "--in");
  const std::string outPath = argument(count, values, "--out");
  const std::string udp = argument(count, values, "--udp");

  if (!submissions.empty() && !journalPath.empty()) {
    return offline(submissions, journalPath, packets, acksPath,
                   flagged(count, values, "--end-session"));
  }
  if (!inList.empty() && !acksPath.empty() && !outPath.empty() && !journalPath.empty()) {
    return live(inList, acksPath, outPath, journalPath, udp);
  }
  std::fprintf(stderr,
               "usage: sequencer --submissions FILE --journal J [--packets P] [--acks A]"
               " [--end-session]\n"
               "       sequencer --in R1,R2 --acks A1,A2 --out RING --journal J"
               " [--udp HOST:PORT]\n");
  return 2;
}
