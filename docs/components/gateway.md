# Component: the gateway

Where clients meet the venue. A gateway terminates sessions, translates a client's order entry
message into the canonical command, forwards it to the sequencer under the gateway's own sequence
numbering, and routes execution reports back to the session that owns each order. A gateway holds
no venue state: it reads the sequence back like every other consumer, which is how it learns the
fate of what it forwarded (P-1).

## The design

The session layer follows SoupBinTCP (Nasdaq, public specification): login with a session and an
expected sequence number, sequenced messages the server replays from where the client left off,
unsequenced messages for the client's own sends, heartbeats both ways, and a clean end of
session. The payload vocabulary follows OUCH (Nasdaq, public specification) in shape and this
venue's schema in encoding, the same move iLink 3 made for CME: a session protocol wrapped around
SBE messages.

The gateway's hard jobs are bookkeeping under failure. Client to venue: every forwarded command
carries the gateway's per-gateway sequence, acks from the sequencer confirm durability, and after
a sequencer failover the gateway resubmits everything unacknowledged, relying on the sequencer's
deduplication (the sequencer page owns that invariant). Venue to client: the gateway consumes the
event stream, filters by ownership, translates to the client's report format, and numbers each
session's stream so a reconnecting client can ask for a replay from its last seen sequence, which
is the client-side mirror of the venue's own rewind mechanism.

The I/O discipline is part of the education: epoll or io_uring event loops, one thread per some
number of sessions, TCP_NODELAY and the smallest number of syscalls per message the API allows.
Kernel bypass (OpenOnload, DPDK) is the production step beyond and is documented rather than
built, since commodity hardware teaches the shapes without it.

## The questions it answers

What a session protocol owes both sides under disconnection and failover; where exactly-once
semantics actually live; and what the operating system's network path costs per message, measured
from socket read to sequencer ring write.

## How it lands

Protocol and doc additions for the session layer; the session state machine with login, replay,
heartbeats and teardown, tested against scripted disconnections; the translation layer; the
resubmission-after-failover suite driven by the sequencer's chaos machinery; and the measured
socket-to-ring hop.

## Where to learn

- Nasdaq SoupBinTCP specification (public). Short, complete, and the template for the session
  layer.
- Nasdaq OUCH specification (public). The order entry vocabulary and its report semantics.
- CME iLink 3 documentation (public). Binary order entry over SBE, with a session layer designed
  decades after OUCH; comparing the two is a course in protocol evolution.
- FIX protocol specifications (FIX Trading Community). What most of the industry actually speaks
  at the edges; worth knowing even where this venue does not use it.
- Dan Kegel, The C10K problem. The historical framing of event-driven network servers.
- Jens Axboe, Efficient IO with io_uring, 2019. The modern Linux I/O path.
- OpenOnload (AMD) and DPDK documentation. What the production step beyond the kernel looks like.
