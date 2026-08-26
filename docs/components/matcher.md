# Component: the matcher

Built. The design lives beside the code in `components/matcher/README.md`, the field-level
contract in [PROTOCOL.md](../PROTOCOL.md), and the proofs in `components/matcher/test/`. This
page carries what the component teaches and where its ground is.

## What it is

One thread on one pinned core, every instrument of its partition, consuming sequenced commands
from a ring and emitting events into another, allocating nothing after an instrument is defined.
An order is an int slot in a hot/cold slab; a side's levels are a flat ladder in folded rank
space under a three level occupancy bitmap; the name index is interleaved open addressing; the
taker lives in registers for the whole walk; events encode in place and publish once per command.

## The questions it answers

What does a full venue remit cost per command when the representation is chosen for the cache
rather than for the type system. Where do the nanoseconds actually go: the loads whose addresses
depend on other loads, the branches the predictor cannot learn, the visibility operations per
event. And what does it take to prove an engine correct without trusting it: a corpus of blessed
behaviour, invariants that hold every cache to the structure it summarises, byte-determinism, and
an allocator that counts.

## Where to learn

- Fowler, The LMAX Architecture, 2011; Thompson et al., Disruptor technical paper, 2011. The
  architecture the matcher's process shape follows.
- Nasdaq OUCH and TotalView-ITCH specifications (public). The command and event vocabulary
  conventions; reading them next to PROTOCOL.md shows which decisions are conventions and which
  are this venue's.
- CME iLink 3 and the FIX Trading Community's Simple Binary Encoding specification. Binary order
  entry done as a public standard, and the encoding this venue uses everywhere.
- Larry Harris, Trading and Exchanges, 2003, chapters on order types, the trading session and
  auctions. Why icebergs, stops, bands and crosses exist at all.
- Drepper, What Every Programmer Should Know About Memory, 2007; Agner Fog's manuals; the Intel
  64 and IA-32 Architectures Optimization Reference Manual. The cache lines, TLBs and branch
  machinery the slab and ladder are shaped for.
- Gil Tene, How NOT to Measure Latency. Why the harness keeps raw series and manifests.

## Industry implementations worth studying

Every serious venue's matcher is closed, so the study path is specifications plus the open
high-performance lineage: the LMAX Disruptor (Real Logic, open source), Aeron's buffers and
claim-commit idiom, and HdrHistogram for the recording side. The public specs above describe the
behaviourally identical surface of Nasdaq's and CME's engines, which is as close as the outside
gets.
