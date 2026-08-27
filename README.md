# Exchange

An electronic exchange: deterministic processes around one total order of messages.

Every command a client sends is assigned one place in a single global sequence, and everything the
venue does, matching, market data, execution reports, recovery, failover, is a function of that
sequence. A process that consumes the same sequence computes the same state, which is what makes a
warm standby a consumer like any other and recovery a replay rather than a subsystem (Schneider
1990, Implementing Fault-Tolerant Services Using the State Machine Approach). The topology, and
the citation each decision stands on, is in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Layout

```
docs/       the architecture, the principles, the protocol, the practice, and the programme
schema/     the SBE schema owning every message that crosses a process boundary
components/ one directory per process, and common/ for the carriers they share
corpus/     behavioural fixtures with their blessed events
scripts/    measurement and analysis
```

The programme in [docs/components/](docs/components/README.md) carries one page per component:
its design, its proofs, how it lands, and where to learn the ground it stands on.

The matcher is the venue's hot core: one thread on one pinned core, consuming sequenced commands
from a ring, emitting events into another, allocating nothing after initialisation. Its design and
its proofs live in `components/matcher/`.

The sequencer is where the order is made: one thread arbitrating gateway submissions, giving each
command its place in the sequence and the venue's one timestamp, journaling it, and publishing it
to the ring and as ranges with heartbeats and rewind. Every downstream process reads the stream
through the consumer library in `components/common/`, from the ring, the journal or the packet
feed, behind one gap-free contract. Under the safe durability policy a standby consumes the
replication link and must cover a command before the world hears it, so published is a subset of
replicated and failover transfers nothing. Its design and its proofs live in
`components/sequencer/`.

The gateway is where clients meet the venue: sessions in SoupBinTCP's shape carried as this
schema's messages, the command vocabulary spoken upstream with the session's identity written in
place, the venue's own events numbered back per session so a reconnection replays from the
sequence it names, and resubmission after a failover made harmless by the sequencer's dedupe. Its
parsers are the venue's first taste of untrusted bytes, so they are fuzzed as well as tested. Its
design and its proofs live in `components/gateway/`.

Market data is the venue seen from outside: a builder derives the visible book from the
matcher's events and publishes an ITCH-shaped feed whose messages structurally cannot carry
attribution, on identical A and B packet feeds with retransmission for what both lose, snapshots
served to late joiners in queue priority order so a replay rebuilds the book's fairness, and a
conflated top of book beside the depth. Its design and its proofs live in
`components/marketdata/`.

## Build

Requires CMake and a C++23 compiler.

```
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build
```

Configuring points git at the committed hooks, so formatting on commit installs itself for anyone
who builds. Formatting is `clang-format` at a hundred columns, pinned by version in
`.clang-format-version`, and the ci workflow fails on a file the pinned version would change, so
the format is part of the build rather than a habit.

## Conventions

Commit messages read `(category): what I am actually doing`. Branches are one change each and land
through a pull request, and every merge to main is gated on green checks: build, tests, the
sanitizer battery, and the pinned format sweep.

Prices and quantities are scaled integers everywhere, nothing on a hot path allocates after
initialisation, no process behind the sequencer reads a clock, and every claim about behaviour or
performance names the mechanism that proves it. Why the code is shaped the way it is lives in
[PRINCIPLES.md](docs/PRINCIPLES.md); what crosses each process boundary lives in
[PROTOCOL.md](docs/PROTOCOL.md); the mechanisms around the code, the flavours, the rituals, the
toolchain and the box, live in [PRACTICE.md](docs/PRACTICE.md).
