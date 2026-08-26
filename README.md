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
docs/       the architecture, the principles, the protocol, and the component programme
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
through a pull request, and every merge to main is gated on green checks: build, tests, and the
pinned format sweep.

Prices and quantities are scaled integers everywhere, nothing on a hot path allocates after
initialisation, no process behind the sequencer reads a clock, and every claim about behaviour or
performance names the mechanism that proves it. Why the code is shaped the way it is lives in
[PRINCIPLES.md](docs/PRINCIPLES.md); what crosses each process boundary lives in
[PROTOCOL.md](docs/PROTOCOL.md).
