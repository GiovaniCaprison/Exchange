#!/bin/sh
# The measurement box, tuned and verified. Written for a c6i.metal (two sockets of Ice Lake,
# 64 physical cores, hyperthread siblings 64-127 pairing core N with N+64) and safe on anything
# close. The campaign's law is that every claim about the environment is checked and recorded,
# so the check mode prints what actually took and the harnesses' manifests carry the truth
# either way (docs/PRACTICE.md).
#
# Usage, on the box, as root:
#   scripts/box_setup.sh boot      # writes kernel parameters and grub; reboot afterwards
#   scripts/box_setup.sh runtime   # governor, turbo, huge pages, IRQ steering; run after boot
#   scripts/box_setup.sh check     # prints the state the manifests will corroborate
#
# The isolated set lives on the second socket, away from the kernel's own housekeeping, and the
# processes are pinned inside it: driver, sequencer, standby, matcher each on their own core,
# producer and consumer of any one ring on the same socket, siblings left idle.

ISOLATED="${ISOLATED:-32-47}"

boot() {
  PARAMETERS="isolcpus=${ISOLATED} nohz_full=${ISOLATED} rcu_nocbs=${ISOLATED}"
  PARAMETERS="${PARAMETERS} processor.max_cstate=1 intel_idle.max_cstate=0"
  echo "GRUB_CMDLINE_LINUX_DEFAULT=\"${PARAMETERS}\"" > /etc/default/grub.d/exchange.cfg
  update-grub
  echo "kernel parameters staged: ${PARAMETERS}"
  echo "reboot, then run: scripts/box_setup.sh runtime"
}

runtime() {
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    echo performance > "${governor}" 2>/dev/null
  done
  # A fixed frequency compares across runs; turbo would donate a thermally variable prize.
  if [ -w /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
    echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
  fi
  # The rings advise huge pages; the kernel obliges shared mappings when shmem THP is on, and
  # the rings themselves belong on /dev/shm.
  if [ -w /sys/kernel/mm/transparent_hugepage/shmem_enabled ]; then
    echo advise > /sys/kernel/mm/transparent_hugepage/shmem_enabled
  fi
  # Interrupts are herded off the isolated cores; the default affinity catches new ones.
  HOUSEKEEPING="1"
  echo "${HOUSEKEEPING}" > /proc/irq/default_smp_affinity 2>/dev/null
  for irq in /proc/irq/[0-9]*; do
    echo "${HOUSEKEEPING}" > "${irq}/smp_affinity" 2>/dev/null
  done
  echo "runtime tuning applied; verify with: scripts/box_setup.sh check"
}

check() {
  echo "cmdline:            $(cat /proc/cmdline)"
  echo "governor (cpu0):    $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unavailable)"
  echo "no_turbo:           $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo unavailable)"
  echo "shmem thp:          $(cat /sys/kernel/mm/transparent_hugepage/shmem_enabled 2>/dev/null || echo unavailable)"
  echo "isolated (kernel):  $(cat /sys/devices/system/cpu/isolated 2>/dev/null || echo none)"
  echo "sockets:            $(lscpu 2>/dev/null | grep 'Socket(s)' || echo unavailable)"
}

case "$1" in
  boot) boot ;;
  runtime) runtime ;;
  check) check ;;
  *) echo "usage: scripts/box_setup.sh boot|runtime|check"; exit 2 ;;
esac
