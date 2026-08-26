# Component: the sequencer

The heart of the venue and the next component built. Every other process borrows its fault
tolerance from determinism; the sequencer is where that determinism is manufactured, so it is the
one component that must survive its own death by mechanism rather than by replay. The organizing
idea: the sequencer converts nondeterminism into a recorded fact. Arrival order across gateways
is racy and clock reads are unrepeatable; the sequencer makes one committed choice about both,
journals it, replicates it, and publishes it, and from that point on the whole venue is a pure
function (Lamport 1978; Schneider 1990).

## The design

Input. Each gateway submits commands over its own ring, stamped with the gateway's id and a
per-gateway sequence number. One sequencer thread arbitrates across the rings; whatever order the
arbiter picks is the order, by definition, so arbitration policy is a fairness and latency
question rather than a correctness one. The sequencer deduplicates on (gateway, gatewaySequence)
and acknowledges each command back to its gateway with the global sequence it received. That
tuple is where exactly-once comes from: a gateway that never saw an ack resubmits, and the
deduplication makes the retry harmless. Retry plus dedupe, never hope.

Durability. A sequenced command is safe to publish when losing it has become impossible, and the
production answer is that durable means present in a replica's memory, with disk trailing
asynchronously. The primary sequences, ships to the standby over a replication link, and
publishes only after the standby's acknowledgment, with pipelining and grouped acks so the link
round trip is amortized across commands rather than paid per command. Publish-after-local-write
exists as a second configuration, so the cost of safety is a measured number rather than a
slogan. The journal itself is PROTOCOL.md's format on both nodes.

The invariant failover rests on: published is a subset of replicated. The standby therefore holds
everything any consumer has ever seen, plus possibly a tail that was acknowledged and never
published, which it simply publishes on takeover. Nothing is transferred at failover because
nothing needs to be.

Leadership. At most one sequencer may extend the log, and two nodes cannot decide that alone, so
a witness process grants epoch-numbered leases: a standby takes over only by winning the next
epoch from the witness, every publication packet carries its epoch, and gateways and consumers
reject anything stale. This is a fixed-role primary, standby and witness rather than a
symmetric-peer consensus protocol, which is the shape venues actually run; Raft (Ongaro and
Ousterhout 2014) is the citable comparison, and reading it against this design shows exactly
which of Raft's machinery a fixed-role deployment does not need and why.

Publication. MoldUDP64-shaped packets (Nasdaq, public specification): session, sequence, count,
message blocks, heartbeats when idle, end of session; a rewinder serving sequence ranges from the
journal over a re-request channel. On box the fast path is the shared-memory ring. One consumer
library serves every downstream process, with three interchangeable sources behind one
contiguous-stream interface: the ring, the journal, and the packet feed with gap detection and
re-request. That library is what every real feed handler is, built once and reused by every later
component.

Time in tests. Leases and failure detection rest on timeouts, and timeouts make tests flaky
unless time is injected, so the lease logic runs on a virtual clock under test. The failover
suite kills the primary at scripted points, before replication, after ack and before publish,
mid-packet, partitions it from the witness, and checks the one property every run: the published
stream stitched across epochs is gap free, duplicate free, and contains every command any gateway
was ever acknowledged. The matcher consuming the stitched stream emits bytes identical to one
consuming an unbroken run, which is the availability story executed (P-2).

The hot path holds the house rules: one thread, zero steady-state allocation under the probe,
claim in place from ingestion to journal buffer to publication packet, and a harness reporting
sequencing overhead per command at offered rates, per durability policy.

## How it lands

1. Protocol and standing-document changes: the input envelope and acknowledgment, epochs on
   publication, replication and lease messages, packet framing; the schema grows the same.
2. The single-node sequencer: arbitration, dedupe, journal, publication with heartbeats, the
   rewinder, and the consumer library with all three sources; corpus-style tests, the allocation
   probe, and the harness.
3. Replication: the pipelined primary-to-standby link, both durability configurations, measured.
4. The witness, leases, epochs, takeover, gateway resubmission, and the chaos suite on the
   virtual clock.
5. The campaign: sequencing overhead and the price of safety, plus the first two-hop path,
   driver in to matcher in.

## Where to learn

- Lamport, Time, Clocks, and the Ordering of Events in a Distributed System, 1978. Total order
  as the foundation; short and worth reading twice.
- Schneider, Implementing Fault-Tolerant Services Using the State Machine Approach, 1990. The
  paper this venue's whole availability story is an implementation of.
- Ongaro and Ousterhout, In Search of an Understandable Consensus Algorithm (Raft), 2014. Leader
  election, log replication and epochs (terms) in their general form; the comparison that
  explains this design's simplifications.
- Lamport, Paxos Made Simple, 2001. The older general form, for contrast.
- Nasdaq MoldUDP64 specification (public). Short; the publication mechanism verbatim.
- Brian Nigito, How to Build an Exchange, Jane Street Tech Talk, 2017. A sequencer-centric venue
  described by an operator; the closest public description of this exact architecture.
- Aeron Cluster (Real Logic, open source) and its design documentation. The engineering-grade
  open implementation of a replicated log with a single leader; the best code to read beside
  this component.
- Scott Patterson, Dark Pools, 2012. Island and INET, the lineage of sequenced-stream venues.

## Industry implementations worth studying

Aeron Cluster is the one open, production-quality implementation of this component's shape and
repays a full read. Nasdaq's INET is the closed original, visible through MoldUDP64, SoupBinTCP
and the ITCH and OUCH specs that surround it. Kafka is the widely known replicated log and a
useful contrast for what a venue does differently: one writer, fixed roles, microsecond budgets,
and consumers that must never diverge.
