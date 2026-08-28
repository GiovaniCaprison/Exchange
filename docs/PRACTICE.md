# Practice

The engineering practice around the code. The standing rule everywhere in this repository is that
nothing load bearing rests on habit: every rule is a mechanism that fails a build, and every
performance belief is a number with a manifest (P-14). This page names the mechanisms that live
around the components rather than inside them: the build flavours, the rituals, the toolchain
policy, the box, and what may gate a merge.

## The sanitizer flavour

`EXCHANGE_SANITIZE` puts the address and undefined behaviour sanitizers on every target,
including the fetched test framework, with recovery disabled so a finding is an exit code. The ci
workflow runs the whole battery under the flavour in debug beside the release build. Undefined
behaviour earns a dedicated build because it is the bug class the optimiser is licensed to assume
away: a program that exhibits it can pass every test at one compiler version and change behaviour
at the next, so it is hunted by instrumentation rather than waited for (Serebryany, Bruening,
Potapenko, Vyukov, AddressSanitizer, USENIX ATC 2012; LLVM UndefinedBehaviorSanitizer
documentation). The fuzzer flavour, `EXCHANGE_FUZZ`, builds the coverage-guided arm of the
gateway's hostile suite under a clang that ships the libFuzzer runtime, and ci runs a bounded
pass on every merge, so the parsers that face the internet are searched as well as tested.

## The codegen ritual

`scripts/codegen.py` reads the hot path's assembly on the box's terms. It compiles a probe
translation unit that explicitly instantiates one matcher partition against a ring that costs
nothing, which forces the hot path into named symbols with everything below inlined in, emits
x86-64 assembly at the box's `-march` whatever machine it runs on, and reports every call and
tail call left inside the chosen functions, aggregated by target. macOS and Linux share the
System V AMD64 calling convention, so a laptop reads the same instruction stream the box runs
without a Linux sysroot.

The reading is the point. A call left in a hot function is either understood and blessed or a
regression: the compiler outlines throw paths as cold continuations, keeps big engine phases as
direct predictable calls, and inlines a small constant-length `std::memcpy` to plain moves, and
each of those is visible in the extract. The assembly shows every escape the code could take; the
allocation probe proves which of them ever fire in steady state; the two mechanisms are read
together. The standing observation for the matcher: nothing on the continuous trading path
reaches `memcpy`, `memset` or the allocator, and every allocation escape in the translation unit
attributes to initialisation, snapshot save and restore, or scratch growth the probe holds
quiescent.

## The toolchain

Development machines build with whatever current compiler they have, and the repository pins what
decides bytes on the wire or in a diff: the formatter and linter versions in
`.clang-format-version`, the schema tool by checksum, the test framework by tag. No build file
names an architecture, because the repository must build on any development machine. The box is
different: campaign builds compile with `-march=native` and one compiler version pinned for the
campaign's duration, and both land in every manifest the run writes, because a compiler upgrade
moves performance by percents and a number whose toolchain is unknown compares to nothing.

## The box

Measurements that mean anything come from one machine: an m5zn.metal instance, two sockets of
Cascade Lake x86-64 with 24 physical cores at the highest all-core clocks EC2 sells, full
performance counter access and an invariant TSC, bare metal so no hypervisor sits between the
process and the machine. The family exists for exactly this workload and the clock is the point:
a single-threaded hot path converts frequency into latency almost linearly. If the venue ever
outgrows 24 cores, the wire-to-wire campaign moves to a many-core metal instance and starts its
numbers fresh, since latency does not compare across microarchitectures. The deployment shape
follows the hardware: a partition and the rings it touches live on one socket with memory touched first from
the core that owns it, hyperthread siblings stay idle, interrupts and housekeeping are herded to
the other socket, and the setup script records what actually took so a manifest can say whether
the isolation asked for was the isolation received. The rings advise transparent huge
pages outright and deployment puts them on tmpfs where the kernel can oblige; explicit 2MB and
1GB pages for the slab remain a campaign question, and so does 128 byte separation of
write-shared fields, since Intel's L2 spatial prefetcher pulls cache lines in pairs. Off box, a VPC carries no
native multicast, which touches the packet publication story only when the venue leaves the
machine.

## The campaign

The campaign is a process, run start to finish on the box, and every number it produces carries
the manifest that makes it checkable. Provision the reference box, an m5zn.metal, with the
current Ubuntu LTS, install the toolchain, and record the compiler version: that version is
pinned for the campaign's duration. Rehearsals work identically on any Intel bare metal, because
nothing here names an architecture until `-march=native` reads the machine it stands on and the
manifest records what it read; the one law is that a campaign's numbers all come from one
instance type, since latency does not compare across microarchitectures. ARM metal is a
different instruction set and no part of the x86 reading applies; virtualised instances have no
performance counters and a hypervisor's jitter, so they are for correctness only, which the
deterministic suites pass anywhere. Tune with `scripts/box_setup.sh`: boot mode stages the kernel parameters (isolated
cores, no tick, shallow C-states), runtime mode sets the governor, disables turbo, enables shmem
transparent huge pages and herds interrupts onto the housekeeping cores, and check mode prints
what actually took. Ring files live on /dev/shm, where the rings' huge page advice lands.

Build the flavours, each recorded into every manifest through the build field: the baseline is
Release with `-march=native`; `EXCHANGE_LTO` adds link time optimisation; `EXCHANGE_PGO`
generates on one full run and uses on the next; BOLT rewrites the hottest binaries post link
from a `perf record -j any,u` profile. Then the runs, every process pinned to its own isolated
core with any one ring's producer and consumer on the same socket. The matcher's cost:
`flowgen` then `matcher-benchmark`. The sequencer's cost and the price of durability in one
process: `subgen` then `sequencer-benchmark` under policies none, local and safe. The venue's
cost as a client feels it: `driver`, the live `sequencer`, and the live `matcher` as separate
pinned processes, measuring driver write to first answering event, closed loop; run it with the
sequencer under the local policy, then under safe with the standby process on its own core, and
the difference between those two runs is the price of safety across real cores. Results
directories hold the raw series and manifests; `scripts/summarize.py` reads them.

The run matrix, literally, with rings on /dev/shm and every process on its own isolated core;
the flavours repeat it with their options on the configure line and their labels on the runs:

```
scripts/box_setup.sh check
cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-march=native
cmake --build build -j

build/components/matcher/flowgen --journal flow.exj --commands 2000000 --seed 7
build/components/matcher/matcher-benchmark --journal flow.exj --results results/campaign \
  --core 12 --label matcher

build/components/sequencer/subgen --submissions subs.exj --commands 2000000 --seed 7
build/components/sequencer/sequencer-benchmark --submissions subs.exj \
  --results results/campaign --core 12 --policy none --label seq-none
build/components/sequencer/sequencer-benchmark --submissions subs.exj \
  --results results/campaign --core 12 --policy local --label seq-local
build/components/sequencer/sequencer-benchmark --submissions subs.exj \
  --results results/campaign --core 12 --policy safe --label seq-safe

build/components/marketdata/marketdata-benchmark --results results/campaign --core 12
build/components/gateway/gateway-benchmark --results results/campaign --core 12

build/components/sequencer/driver --gateway /dev/shm/gw.ring --acks /dev/shm/ack.ring \
  --events /dev/shm/events.ring --results results/campaign --core 12 --label twohop-local &
build/components/sequencer/sequencer --in /dev/shm/gw.ring --acks /dev/shm/ack.ring \
  --out /dev/shm/seq.ring --journal seq.exj &
build/components/matcher/matcher --in /dev/shm/seq.ring --out /dev/shm/events.ring \
  --journal matcher.exj &
wait %1 && kill %2 %3
```

Pin the sequencer and matcher too (`taskset -c 13` and `-c 14` before their commands), rerun the
two-hop with the sequencer under `--policy safe --replicate /dev/shm/rep.ring --replicate-acks
/dev/shm/repack.ring` and a `sequencer --standby-in /dev/shm/rep.ring --standby-acks
/dev/shm/repack.ring --journal standby.exj` process on core 15, and the difference between the
two driver runs is the price of safety across real cores. Profile guidance is two passes of the
same matrix, `-DEXCHANGE_PGO=generate` then `-DEXCHANGE_PGO=use`; post-link layout is one
command per measured binary, `BOLT_MODE=perf scripts/bolt.sh BINARY -- WORKLOAD-ARGS`, whose
whole cycle ci proves in instrumentation mode on every merge.

The wire-to-wire arm is the venue measured as a venue: client order entry in to acceptance heard
back, over real TCP and UDP, under flow that reacts to the book because the bots do. One command
runs it, `SPIN=1 DURATION=60 NOISE=3 MULTICAST=239.7.7.7 scripts/day.sh results/campaign/day`,
with the gateway pinned by prefixing its line in the script with `taskset -c 16` on the box.
`SPIN=1` matters: the rings cannot wake a socket wait, so an unspun gateway ages every acceptance
by its poll timeout, and the difference between the spun and unspun day is itself a measurement
worth keeping. The maker's manifest lands beside the journals; for a dense series give a noise
seat `--results`, because replaces keep their ids and measure nothing while every taker order is
a new acceptance. Decomposition runs from the outside in: the bot's wire-to-wire number, minus
the driver's two-hop ring-to-ring number, is the price of the socket edges; the two-hop number
minus the per-component machine costs is the price of the carriers, and every term has its own
harness in the matrix above.

## The second box

Replication earns its name when the standby is a machine away. Provision a second m5zn.metal in
the same placement group, run `scripts/box_setup.sh` on both, and time both with PTP, because
one-way latency between machines is meaningless on unsynchronized clocks and learning why is
half the value: `sudo apt install linuxptp`, then on each box `sudo ptp4l -i INTERFACE -m` with
one grandmaster and `sudo phc2sys -s INTERFACE -w -m` to steer the system clock; `pmc -u -b 0
'GET CURRENT_DATA_SET'` shows the offset, and the campaign records it in the manifest the way it
records isolation.

The invocation is the socket suite's, with a real address in place of loopback. On the standby
box: `sequencer --standby-udp 36301 --repair-udp PRIMARY_HOST:36302 --journal standby.exj`. On
the primary box, the live line gains `--policy safe --replicate-udp STANDBY_HOST:36301
--repair-port 36302`. Ranges cross one way, acknowledgments and rewind requests ride back on the
repair port, loss is repaired by the reship and reordered arrivals by the relink, and at the
close the journals must be byte-identical across the two machines, which is the same diff the
suite runs on loopback every merge. The two-hop driver run against a cross-box safe policy, next
to the same run against the on-box one, is the measured price of the wire.

The witness still leases over rings, so automated failover remains an on-box arrangement; the
witness's own UDP carrier is the named next step on the horizon page.

The campaign feeds back into the repository: baselines become budgets and budgets become gates.
The codegen ritual is already one: `scripts/codegen.py --gate scripts/codegen-blessed.txt` runs
on every merge, holding each watched hot function to a blessed ceiling of instructions and
escapes, generous today because instruction counts drift across compilers, and tightened by the
campaign once the box gives normal a number. Raising a ceiling is a deliberate act that belongs
in the diff that needed it, with the ritual's report beside it.

## What may gate

Every merge is gated on the release build, the battery, the sanitizer flavour, the pinned linter
and the pinned format sweep. Wall time on a shared runner gates nothing, because the runner is
nobody's box and a latency number without isolation is noise wearing a suit. The campaign is
where enforcement grows: once baselines exist on the box, instruction count and cache behaviour
per command become assertable budgets, and the codegen ritual's blessed-escape list can harden
from a reading into a check. Profile guided optimisation, link time optimisation and post-link
layout (BOLT) enter the same way, as measured comparisons whose flags live in the manifest rather
than as folklore defaults.

## Where to learn

- Carl Cook, When a Microsecond Is an Eternity: High Performance Trading Systems in C++, CppCon
  2017. The practice this page implements, from inside a firm that lives it.
- David Gross, When Nanoseconds Matter: Ultrafast Trading Systems in C++, CppCon 2024. The same
  ground a generation later, with the measurement discipline front and centre.
- Matt Godbolt, What Has My Compiler Done for Me Lately? Unbolting the Compiler's Lid, CppCon
  2017. Reading assembly as a habit; the codegen ritual is this talk as a script.
- Chandler Carruth, Tuning C++: Benchmarks, and CPUs, and Compilers! Oh My!, CppCon 2015. Why
  benchmarks lie and how to make them stop.
- Serebryany, Bruening, Potapenko, Vyukov, AddressSanitizer: A Fast Address Sanity Checker,
  USENIX ATC 2012. The mechanism under the sanitizer flavour.
- Panchenko, Auler, Nell, Ottoni, BOLT: A Practical Binary Optimizer for Data Centers and Beyond,
  CGO 2019. What post-link layout buys and why, for the campaign.
- Intel 64 and IA-32 Architectures Optimization Reference Manual. The box's own book.
