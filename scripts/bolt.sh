#!/bin/sh
# Post-link layout for a measured binary (docs/PRACTICE.md, the campaign): BOLT reorders the
# binary's code from a profile of it actually running, which buys what the compiler could not
# know at link time (Panchenko et al., CGO 2019). Two profile modes: instrument needs no
# performance counters, so it runs anywhere Linux runs and ci proves the whole cycle with it;
# perf uses the real cycles profile and is the box's mode. The measured binaries link with
# --emit-relocs on Linux precisely so BOLT can move their code.
#
#   scripts/bolt.sh BINARY -- WORKLOAD-ARGS...
#
#   BOLT_MODE=instrument|perf   (default instrument)
#   LLVM_BOLT=llvm-bolt         PERF2BOLT=perf2bolt        PERF=perf
#
# The result lands beside the input as BINARY.bolt; run the same workload through both and the
# difference is the price the compiler's layout was paying.

set -e

BINARY="$1"
shift
if [ "$1" != "--" ] || [ -z "$BINARY" ]; then
  echo "usage: scripts/bolt.sh BINARY -- WORKLOAD-ARGS..." >&2
  exit 2
fi
shift

MODE="${BOLT_MODE:-instrument}"
BOLT="${LLVM_BOLT:-llvm-bolt}"
WORK="$(mktemp -d)"
PROFILE="${WORK}/profile.fdata"

if [ "$MODE" = "perf" ]; then
  "${PERF:-perf}" record -e cycles:u -j any,u -o "${WORK}/perf.data" -- "$BINARY" "$@"
  "${PERF2BOLT:-perf2bolt}" -p "${WORK}/perf.data" -o "$PROFILE" "$BINARY"
else
  "$BOLT" "$BINARY" -instrument -o "${BINARY}.inst" \
    --instrumentation-file="$PROFILE"
  "${BINARY}.inst" "$@"
  rm -f "${BINARY}.inst"
fi

"$BOLT" "$BINARY" -o "${BINARY}.bolt" -data="$PROFILE" \
  -reorder-blocks=ext-tsp -reorder-functions=cdsort -split-functions -split-all-cold \
  -dyno-stats
rm -rf "$WORK"
echo "wrote ${BINARY}.bolt"
