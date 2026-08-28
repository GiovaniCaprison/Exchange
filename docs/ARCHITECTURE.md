# Architecture

The venue is a set of deterministic processes around one total order of messages. The sequence is
the spine: every command is assigned a global sequence number exactly once, every process consumes
that sequence, and every fact the venue publishes is derived from it.

## The total order

A replicated service stays consistent when its replicas apply the same deterministic operations in
the same order. Total ordering of events is the foundation (Lamport 1978, Time, Clocks, and the
Ordering of Events in a Distributed System), and building the service as a state machine fed by
that order is what makes replication, recovery and failover the same mechanism (Schneider 1990,
Implementing Fault-Tolerant Services Using the State Machine Approach).

The sequencer is where the order is made. It assigns every inbound command a global sequence
number, stamps it with the venue's one timestamp, journals the sequenced stream, and publishes it
to every consumer. Publication is sequenced with rewind: a consumer that misses a message
re-requests the gap by sequence range, and a consumer that starts late reads forward from the
journal. The mechanism follows MoldUDP64 (Nasdaq, public specification).

## The processes

Gateways terminate client sessions: framing, authentication, heartbeats, and the translation of a
client's order entry message into the venue's canonical command. Session framing and the order
entry vocabulary follow the shape of SoupBinTCP and OUCH (Nasdaq, public specifications); carrying
the same vocabulary over SBE follows iLink 3 (CME Group, public documentation). A gateway holds no
venue state: it forwards commands to the sequencer and reads the sequence back like every other
consumer, which is how it learns the fate of what it forwarded. Under a sequencer failover it
resubmits everything unacknowledged under the same per-gateway numbering, and the deduplication
the new leader inherited makes every retry harmless, so exactly-once holds from the client's
chair as well.

Matcher partitions own the books. Each partition is one process with one thread on one pinned
core, consuming the sequenced stream through a ring, matching, and emitting events into another
ring. A single writer keeps a book in one core's cache and needs no locks, no fences on the hot
path and no retry loops, which makes one thread the fast option as well as the simple one (Fowler
2011, The LMAX Architecture; Thompson, Farley, Barker, Gee, Stewart 2011, Disruptor technical
paper, LMAX). Symbols are partitioned across matchers, so concurrency exists between books and
never within one, and a partition serves every instrument routed to it from the one thread. A
sharded deployment runs one matcher per instrument set on the same sequenced stream, each
filtering to what it serves, journaling only that, and minting ids under its shard's namespace,
so downstream consumers merge the event streams without a thought; a mass cancel naming
instrument zero sweeps every shard, because it addresses every book wherever it lives.

Downstream consumers derive everything else from the sequenced stream and the event streams: the
public market data feed, built as its own process in TotalView-ITCH's shape with A and B packet
feeds, retransmission and snapshot-then-join recovery (Nasdaq, public specifications), private
execution reports through the gateways, drop copy sessions serving each participant's events to
the firms responsible for it, and surveillance watching the whole stream for the patterns
regulation exists to catch, each a process consuming the same events as everyone. Trading states move only on
SessionControl, and the market operations scheduler is its one sender: the market's clock is a
consumer with authority, submitting through the same path as everything else. Nothing downstream ever executes inside
the matcher: a consumer that needs a book builds one from the events, which is also the standing
proof that the event stream is sufficient to rebuild the state it describes.

## Encoding and transport

Every message that crosses a process boundary is FIX Simple Binary Encoding (FIX Trading
Community, Simple Binary Encoding specification), generated from the one schema in `schema/`.
Fields are ordered for decode cost and alignment rather than for prose, and the field-level
contract is in [PROTOCOL.md](PROTOCOL.md).

On one box, transport is shared-memory single-producer single-consumer rings with space claimed in
place, so a producer encodes directly into the bytes its consumer will read and no message is ever
copied between processes on the hot path. The matcher's event stream alone is a broadcast ring,
one producer and any number of independent readers with no back pressure, because the gateway,
the market data publisher and the operations scheduler all consume it and none of them may stall
the matcher. Off box, the reference is Aeron (Real Logic, open source messaging), which carries
the same sequenced-stream shape between machines.

## Time

Time is stamped once. The sequencer stamps each command as it sequences it, every process carries
that timestamp forward, and no process behind the sequencer reads a clock. A clock read inside a
consumer is a nondeterministic input, and a state machine replica is only a replica while its
inputs are the sequence and nothing else (Schneider 1990). Stamping once is therefore what makes
replay exact: a journaled stream replayed tomorrow carries today's timestamps and produces today's
bytes.

## Availability

High availability is a property of the architecture rather than a feature. A warm twin matcher
consumes the same sequenced commands as the primary, over the packet feed with the rewinder as
its repair, and publishes its own event ring and journal, byte identical to the primary's
because the engine is a pure function of the stream. Consumers seat at both rings and
deduplicate by the event sequence every event carries, so the twin rings are the event stream's
A and B, and the primary dying mid-day is a non-event nobody downstream notices: nothing is
transferred at failover because nothing needs to be, and the failover drill kills the primary
for real on every merge. That is sound exactly when every component is deterministic: the same
sequenced input produces byte identical output, which this repository holds as a tested
requirement on every component rather than an assumption (Schneider 1990).

The sequencer is the one process that cannot borrow its availability this way, because it creates
the sequence rather than consuming it, so its survival is built from three mechanisms. A command
is acknowledged and published only once it is replicated to the standby, so published is a subset
of replicated and the standby always holds everything any consumer has seen. Epoch numbers on
every published range enforce that at most one sequencer extends the log: a stale epoch is
rejected everywhere. And a witness process arbitrates the lease between the two sequencers, since
two nodes alone cannot decide leadership under a partition; the fixed roles of primary, standby
and witness are the deployment venues actually run, and the general form they simplify is leader
based log replication (Ongaro and Ousterhout 2014, Raft). The protocol's leadership section
carries the mechanics.

Recovery composes from the same parts. A process restores its most recent snapshot, replays the
journal suffix from the sequence the snapshot names, and the events it emits are byte identical to
the run that never stopped. The determinism suite in each component exercises exactly that: replay
twice and diff the bytes, then restore mid-stream and diff the suffix.

## What a process may assume

Input arrives sequenced, gap free, well framed and stamped. Those are preconditions established by
the sequencer and the transport, and no process re-checks them on the hot path. Business
validation is a different matter and belongs to whoever owns the rule: the matcher validates an
order against its instrument once at its own boundary, refuses with a machine readable reason, and
trusts everything below that boundary. The gateway's risk gate stands before the sequencer and
owns the client-facing rules, so what it refuses never takes a place in the order and a
cancellation is never blocked; the market access rule it models is SEC Rule 15c3-5.
