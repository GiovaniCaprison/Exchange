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
documentation).

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

Measurements that mean anything come from one machine: a c6i.metal instance, two sockets of Ice
Lake x86-64 with 64 physical cores, full performance counter access and an invariant TSC, bare
metal so no hypervisor sits between the process and the machine. The deployment shape follows the
hardware: a partition and the rings it touches live on one socket with memory touched first from
the core that owns it, hyperthread siblings stay idle, interrupts and housekeeping are herded to
the other socket, and the setup script records what actually took so a manifest can say whether
the isolation asked for was the isolation received. Huge pages, both 2MB and 1GB, are a measured
comparison for the slab and the rings, and so is 128 byte separation of write-shared fields,
since Ice Lake's L2 spatial prefetcher pulls cache lines in pairs. Off box, a VPC carries no
native multicast, which touches the packet publication story only when the venue leaves the
machine.

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
