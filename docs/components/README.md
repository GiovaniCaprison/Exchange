# The component programme

The standing documents describe what the system is. This directory is deliberately different: it
is the programme, one document per component, each carrying the component's full design, the
questions it exists to answer, how it lands as pull requests, and where to learn the ground it
stands on: the primary specifications, the papers, the industry implementations worth studying,
and the books. The owner reads these before building a component and comes back to them after;
they are the study companion the project exists to fill in.

The project's goal is understanding: exchanges and the technology under them, low latency
programming as a discipline, and the applications built on both. The venue is real in every way
that teaches something and deliberately bounded where realism would only add operational bulk:
mechanisms are built 1:1 with what venues run, regulations are modelled and cited rather than
implemented wholesale, and every claim any component makes is proved by a mechanism, never
asserted (P-14).

## The arc

Depth first: each component is taken to production grade, with its proofs and its measurements,
before the next begins.

| Order | Component | One line |
|---|---|---|
| 1 | [matcher](matcher.md) | the hot core: one thread, one book per instrument, zero allocation, byte-determinism |
| 2 | [sequencer](sequencer.md) | the heart: one total order, journaled, replicated, published with rewind, surviving its own death |
| 3 | [gateway](gateway.md) | sessions: login, heartbeats, replay, translation of client order entry into the canonical command |
| 4 | [marketdata](marketdata.md) | the public feed: books rebuilt from events, A and B feeds, retransmission, snapshots |
| 5 | [risk](risk.md) | pre-trade controls inline before sequencing, with a nanosecond budget |
| 6 | [operations](operations.md) | the market's clock: sessions, auctions on schedule, volatility halts |
| 7 | [ecosystem](ecosystem.md) | clients and bots that make the venue trade, and the wire-to-wire campaign |
| 8 | [horizon](horizon.md) | everything after: surveillance, post-trade, off-box transport, other engines |

## The discipline every component carries

One thread per process on the latency path, consuming the sequence (P-1, P-4). Zero steady-state
allocation, held by a counting-allocator probe (P-6). Byte determinism, held by replay-twice and
kill-and-restore suites (P-2). A behavioural corpus where the component makes decisions, and
invariants over generated flow where it keeps state (P-7). A measurement harness writing raw
series and an honest manifest (P-14). One concern per pull request, each green before the next.
The mechanisms that surround every component, the sanitizer flavour, the codegen ritual, the
toolchain policy and the box itself, are the standing document [PRACTICE.md](../PRACTICE.md).

## What each landing changes in the standing documents

The standing documents stay finished-state, so each component's arrival rewrites them rather than
appending futures to them. The ledger of what to touch:

- sequencer: PROTOCOL.md gains the gateway-to-sequencer envelope and acknowledgment, the epoch
  field on publication, the replication and lease messages, and the publication packet framing;
  the schema grows the same; ARCHITECTURE.md's availability section gains the witness, epochs and
  the replicated-before-published rule; README.md's layout and component list gain the sequencer
  and the consumer library.
- gateway: PROTOCOL.md gains the session layer (login, heartbeats, session sequence, replay) and
  the client-facing order entry translation; ARCHITECTURE.md's gateway paragraph deepens with the
  resubmission-after-failover contract.
- marketdata: PROTOCOL.md gains the public feed message set and the snapshot service;
  ARCHITECTURE.md's downstream paragraph names the built feed rather than the convention alone.
- risk: PROTOCOL.md gains the risk refusal reasons and where they sit in precedence;
  ARCHITECTURE.md's boundary section names the risk layer's position.
- operations: PROTOCOL.md gains halt semantics beyond the session states if any are added;
  ARCHITECTURE.md gains the scheduler as the sender of SessionControl.
- ecosystem: README.md gains the run-a-venue quickstart; METHODOLOGY-style content lands with the
  wire-to-wire campaign if a measurement document is split out by then.

## Reading that spans the whole programme

- Brian Nigito, How to Build an Exchange, Jane Street Tech Talk, 2017. The sequenced-stream
  architecture this venue uses, explained end to end by someone who operated one.
- Larry Harris, Trading and Exchanges: Market Microstructure for Practitioners, Oxford, 2003. The
  market structure canon: why venues have the mechanics the matcher implements.
- Scott Patterson, Dark Pools, 2012. The history of Island and INET, which is the lineage of the
  sequenced-stream design.
- Martin Fowler, The LMAX Architecture, 2011, and Thompson, Farley, Barker, Gee, Stewart, the
  Disruptor technical paper, 2011. The single-threaded-core-behind-rings result.
- Ulrich Drepper, What Every Programmer Should Know About Memory, 2007, and Agner Fog's
  optimization manuals (agner.org). The hardware the nanoseconds live on.
- Gil Tene, How NOT to Measure Latency (talk), and HdrHistogram. Coordinated omission and why the
  measurement discipline here is shaped the way it is.
- Brendan Gregg, Systems Performance, 2nd edition, 2020. The operating system under the process.
- Martin Thompson's Mechanical Sympathy blog. The mindset, named.
