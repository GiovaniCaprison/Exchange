// The sequencer process. Three ways in, one core: a live mode arbitrating gateway rings on the
// wall clock, an offline mode consuming a submission journal on the scripted clock, which is how
// the determinism suite holds two runs to identical bytes and how a sequenced stream is rebuilt
// for audit, and a standby mode consuming the replication link and holding everything any
// consumer has seen. The offline arbitration is the file's order, because a recorded arrival
// order is exactly what the file is.
//
//   sequencer --submissions FILE --journal J [--packets P] [--acks A] [--policy local|safe]
//             [--standby-journal SJ] [--end-session]
//   sequencer --in R1,R2,... --acks A1,A2,... --out RING --journal J [--udp HOST:PORT]
//             [--policy local|safe --replicate RING --replicate-acks RING]
//   sequencer --standby-in RING --standby-acks RING --journal J
//   sequencer --witness --lease-requests R1,R2 --lease-responses S1,S2 [--ttl-ms N]
//
// In live mode the sequencer visits the gateway rings round robin, one record per visit, so no
// gateway's burst starves another; a sweep that sequenced something flushes one packet, and a
// quiet wire gets a heartbeat every hundred milliseconds. Under the safe policy the primary
// creates both replication rings and the standby attaches to them. Offline, the safe policy runs
// an in-process standby over the loopback, one pump per submission. SIGINT or SIGTERM closes the
// session.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "clock.hpp"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/LeaseRequest.h"
#include "exchange_protocol/LeaseResponse.h"
#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "leadership.hpp"
#include "loopback.hpp"
#include "sequencer.hpp"
#include "spsc_ring.hpp"
#include "standby.hpp"

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

std::uint64_t replicationAckOf(const char* message) {
  std::uint64_t upToSequence = 0;
  std::memcpy(&upToSequence, message + sbe::MessageHeader::encodedLength(), sizeof upToSequence);
  return upToSequence;
}

int offline(const std::string& submissionsPath, const std::string& journalPath,
            const std::string& packetsPath, const std::string& acksPath, const bool safe,
            const std::string& standbyJournalPath, const bool endSession) {
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
  sequencing::LoopbackLink link;
  sequencing::LoopbackAcks loopbackAcks;
  std::optional<common::journal::Writer> standbyJournal;
  std::optional<sequencing::Standby<sequencing::LoopbackAcks, common::journal::Writer>> standby;
  if (safe) {
    standbyJournal.emplace(standbyJournalPath.empty() ? journalPath + ".standby"
                                                      : standbyJournalPath);
    standby.emplace(*standbyJournal, loopbackAcks);
  }
  sequencing::Sequencer<DiscardingRing, CapturingRing, FilePacketSink, common::journal::Writer,
                        sequencing::ScriptedClock, sequencing::LoopbackLink>
      sequencer(out, acks, packets, journal, clock, link,
                safe ? sequencing::Durability::SAFE : sequencing::Durability::LOCAL);

  for (std::size_t at = 0; at < submissions.count(); at++) {
    sequencer.onSubmission(submissions.messages.data() + submissions.offsets[at],
                           submissions.lengths[at]);
    if (standby) {
      sequencing::pumpLoopback(sequencer, link, *standby, loopbackAcks);
    }
  }
  if (endSession) {
    sequencer.endSession();
    if (standby) {
      sequencing::pumpLoopback(sequencer, link, *standby, loopbackAcks);
    }
  } else {
    sequencer.flush();
  }

  if (!acksPath.empty()) {
    std::FILE* ackFile = std::fopen(acksPath.c_str(), "wb");
    if (ackFile == nullptr) {
      std::fprintf(stderr, "cannot write acks %s\n", acksPath.c_str());
      return 2;
    }
    for (const CapturingRing& ring : acks) {
      std::fwrite(ring.captured().data(), 1, ring.captured().size(), ackFile);
    }
    std::fclose(ackFile);
  }
  return 0;
}

template <typename Link>
int liveLoop(std::vector<common::SpscRing>& ins, std::vector<common::SpscRing>& acks,
             common::SpscRing& out, common::journal::Writer& journal, UdpSink& packets, Link& link,
             const sequencing::Durability policy, common::SpscRing* replicationAcks) {
  sequencing::WallClock clock;
  sequencing::Sequencer<common::SpscRing, common::SpscRing, UdpSink, common::journal::Writer,
                        sequencing::WallClock, Link>
      sequencer(out, acks, packets, journal, clock, link, policy);

  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  std::uint64_t lastSend = clock.now();
  std::uint64_t lastPublished = 0;
  while (stopped == 0) {
    std::size_t sequenced = 0;
    for (common::SpscRing& in : ins) {
      sequenced += in.pollOne(
          [&](char* record, const std::size_t length) { sequencer.onSubmission(record, length); });
    }
    if (replicationAcks != nullptr) {
      replicationAcks->poll([&](char* message, const std::size_t) {
        sequencer.onReplicationAck(replicationAckOf(message));
      });
    }
    if (sequenced > 0 || sequencer.published() > lastPublished) {
      sequencer.flush();
      lastPublished = sequencer.published();
      lastSend = clock.now();
      continue;
    }
    if (clock.now() - lastSend > 100'000'000ULL) {
      sequencer.heartbeat();
      lastSend = clock.now();
    }
  }
  // A dying primary drains what the standby already covers, and closes only a drained session;
  // an unclosed one is what takeover exists for.
  if (replicationAcks != nullptr) {
    for (int spins = 0; spins < 1'000'000 && sequencer.pending() > 0; spins++) {
      replicationAcks->poll([&](char* message, const std::size_t) {
        sequencer.onReplicationAck(replicationAckOf(message));
      });
    }
  }
  if (sequencer.pending() == 0) {
    sequencer.endSession();
  } else {
    std::fprintf(stderr, "leaving %llu commands unpublished; the standby holds them\n",
                 static_cast<unsigned long long>(sequencer.pending()));
  }
  return 0;
}

int live(const std::string& inList, const std::string& ackList, const std::string& outPath,
         const std::string& journalPath, const std::string& udp, const bool safe,
         const std::string& replicatePath, const std::string& replicateAcksPath) {
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

  if (safe) {
    if (replicatePath.empty() || replicateAcksPath.empty()) {
      std::fprintf(stderr, "the safe policy needs --replicate and --replicate-acks rings\n");
      return 2;
    }
    common::SpscRing link = common::SpscRing::create(replicatePath, 1 << 24);
    common::SpscRing replicationAcks = common::SpscRing::create(replicateAcksPath, 1 << 20);
    return liveLoop(ins, acks, out, journal, packets, link, sequencing::Durability::SAFE,
                    &replicationAcks);
  }
  sequencing::NullLink link;
  return liveLoop(ins, acks, out, journal, packets, link, sequencing::Durability::LOCAL, nullptr);
}

// The witness process: one request ring and one response ring per node, the lease law applied
// on the wall clock. It holds no venue state, which is the point: three small processes decide
// leadership so that two big ones cannot both hold it.
int witnessMode(const std::string& requestList, const std::string& responseList,
                const std::uint64_t ttlNanoseconds) {
  const std::vector<std::string> requestPaths = split(requestList);
  const std::vector<std::string> responsePaths = split(responseList);
  if (requestPaths.size() != responsePaths.size()) {
    std::fprintf(stderr, "every request ring needs its response ring\n");
    return 2;
  }
  std::vector<common::SpscRing> requests;
  std::vector<common::SpscRing> responses;
  requests.reserve(requestPaths.size());
  responses.reserve(responsePaths.size());
  for (const std::string& path : responsePaths) {
    responses.push_back(common::SpscRing::create(path, 1 << 16));
  }
  for (const std::string& path : requestPaths) {
    requests.push_back(common::SpscRing::attach(path));
  }
  sequencing::WallClock clock;
  sequencing::Witness<sequencing::WallClock> witness(clock, ttlNanoseconds);
  constexpr std::size_t RESPONSE_BYTES =
      sbe::MessageHeader::encodedLength() + sbe::LeaseResponse::sbeBlockLength();
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0) {
    for (std::size_t node = 0; node < requests.size(); node++) {
      requests[node].pollOne([&](char* message, const std::size_t length) {
        sbe::MessageHeader wrap;
        wrap.wrap(message, 0, 0, length);
        sbe::LeaseRequest request;
        request.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        const auto answer =
            witness.onRequest(request.nodeId(), request.epoch(), request.lastSequence());
        common::SpscRing& out = responses[node];
        const std::size_t at = out.claim(RESPONSE_BYTES);
        sbe::LeaseResponse response;
        response.wrapAndApplyHeader(out.buffer(), at, at + RESPONSE_BYTES);
        response.ttl(answer.ttl).epoch(answer.epoch).holder(answer.holder);
        response.granted(answer.granted ? 1 : 0);
        out.commit();
        out.publish();
      });
    }
  }
  return 0;
}

int standbyMode(const std::string& inPath, const std::string& acksPath,
                const std::string& journalPath) {
  common::SpscRing in = common::SpscRing::attach(inPath);
  common::SpscRing acks = common::SpscRing::attach(acksPath);
  common::journal::Writer journal(journalPath);
  sequencing::Standby<common::SpscRing, common::journal::Writer> standby(journal, acks);
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);
  while (stopped == 0 && !standby.ended()) {
    in.poll([&](char* range, const std::size_t length) { standby.onRange(range, length); });
  }
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
  const std::string policy = argument(count, values, "--policy");
  const std::string standbyIn = argument(count, values, "--standby-in");
  const std::string standbyAcks = argument(count, values, "--standby-acks");
  const std::string leaseRequests = argument(count, values, "--lease-requests");
  const std::string leaseResponses = argument(count, values, "--lease-responses");
  const bool safe = policy == "safe";
  if (!policy.empty() && policy != "safe" && policy != "local") {
    std::fprintf(stderr, "policy must be local or safe\n");
    return 2;
  }

  if (flagged(count, values, "--witness") && !leaseRequests.empty() && !leaseResponses.empty()) {
    const std::string ttl = argument(count, values, "--ttl-ms");
    return witnessMode(leaseRequests, leaseResponses,
                       (ttl.empty() ? 100 : std::stoull(ttl)) * 1'000'000ULL);
  }
  if (!standbyIn.empty() && !standbyAcks.empty() && !journalPath.empty()) {
    return standbyMode(standbyIn, standbyAcks, journalPath);
  }
  if (!submissions.empty() && !journalPath.empty()) {
    return offline(submissions, journalPath, packets, acksPath, safe,
                   argument(count, values, "--standby-journal"),
                   flagged(count, values, "--end-session"));
  }
  if (!inList.empty() && !acksPath.empty() && !outPath.empty() && !journalPath.empty()) {
    return live(inList, acksPath, outPath, journalPath, udp, safe,
                argument(count, values, "--replicate"),
                argument(count, values, "--replicate-acks"));
  }
  std::fprintf(stderr,
               "usage: sequencer --submissions FILE --journal J [--packets P] [--acks A]"
               " [--policy local|safe] [--standby-journal SJ] [--end-session]\n"
               "       sequencer --in R1,R2 --acks A1,A2 --out RING --journal J"
               " [--udp HOST:PORT] [--policy safe --replicate RING --replicate-acks RING]\n"
               "       sequencer --standby-in RING --standby-acks RING --journal J\n"
               "       sequencer --witness --lease-requests R1,R2 --lease-responses S1,S2"
               " [--ttl-ms N]\n");
  return 2;
}
