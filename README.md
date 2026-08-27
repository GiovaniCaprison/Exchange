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

Risk is the gate in the gateway's path: rate throttle, order size, notional, duplicate, price
collar and credit, cheapest first, refused with a typed reason on the session, and what the gate
refuses never takes a place in the global order (SEC Rule 15c3-5 is the modelled driver). Credit
is a ledger accounted from the admission side and reconciled by the venue's own events, held to
the engine's actual holdings by invariants and to conservation, draining to zero when everything
closes. Its design and its proofs live in `components/risk/`.

Market operations is the market's clock: trading states move only on SessionControl commands, and
the operations scheduler is their one sender, a consumer with authority that reads the same
sequenced stream as everyone and can only act by submitting commands that take effect when
sequenced. It runs the calendar, fires LULD-shaped volatility halts from a trailing band over the
stream's own prints, reopens halted books through an auction, and owns reference data, so an
instrument's definition rides the same acknowledged carrier as the session states. Its design and
its proofs live in `components/operations/`.

The ecosystem is the participant's seat: a feed handler consuming the public feed A/B-arbitrated
with gap repair and snapshot join, an order entry client speaking the session protocol with
replay-exact reconnection, and on top of both the bots that make the venue trade, a market maker
in Avellaneda and Stoikov's shape, noise takers, and a momentum chaser, every one a pure function
of the stream and a seed, so a simulated day driven twice is the same day, journals included. Its
design and its proofs live in `components/ecosystem/`.

Oversight is the venue answerable: drop copy sessions serving each participant's events byte
exactly to the firms responsible for it, on a read-only channel a command poisons, and
surveillance watching the whole stream for wash trades, spoofing and layering, every detection a
pure function of the stream so a replayed day raises the same alerts at the same sequences, with
cases landing in a replayable alert journal. Its design and its proofs live in
`components/oversight/`.

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

## Run a venue

One command runs the whole venue and its participants on this machine, every process real, and
leaves the journals, the logs and the maker's wire-to-wire measurement in a directory:

```
scripts/day.sh trading-day
```

`DURATION=60`, `NOISE=3`, `MULTICAST=239.7.7.7` (a group every bot joins, ITCH's delivery; without
it the two unicast feeds seat two consumers) and `SPIN=1` (busy-poll the gateway, the box's
posture) parameterise the day. The script is the automation; the processes it starts, in
dependency order, are these, and starting them by hand teaches the wiring:

```
build/components/gateway/gateway --listen 36201 --submissions gw.ring --acks gwacks.ring   --events events.ring --participants 7:42,8:43 --gateway-id 0 &
build/components/operations/operations --submissions ops.ring --acks opsacks.ring   --events events.ring --instruments 1 --define 1:5:1:5:1000000:100000000:100000   --calendar 0:CONTINUOUS --gateway-id 1 &
build/components/sequencer/sequencer --in gw.ring,ops.ring --acks gwacks.ring,opsacks.ring   --out seq.ring --journal seq.exj &
build/components/matcher/matcher --in seq.ring --out events.ring --journal matcher.exj &
build/components/marketdata/marketdata --in events.ring --a 127.0.0.1:36202   --b 127.0.0.1:36203 --rewind 36204 &
build/components/ecosystem/bot --connect 36201 --participant 7 --secret 42 --feed 36202   --rewind 36204 --role maker --duration-s 30 --results day --label wire-to-wire &
build/components/ecosystem/bot --connect 36201 --participant 8 --secret 43 --feed 36203   --rewind 36204 --role noise --seed 9 --duration-s 30 --expect-trades
```

The gateway and the scheduler create their submission rings and wait for the rest; the sequencer
attaches them and makes the order; the matcher broadcasts events; market data serves the feed,
retransmission and snapshots; the bots trade. The whole arrangement is also a merge gate: the
ecosystem suite runs exactly this venue as processes and holds it to its word by the crowd's own
exit code.

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
