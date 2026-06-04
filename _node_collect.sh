#!/bin/bash
head -1 /proc/stat
echo ===SEP===
grep -E "^(MemTotal|MemFree|MemAvailable|Buffers|Cached:)" /proc/meminfo
echo ===SEP===
for d in /sys/fs/cgroup/cpuacct/slurm_$(hostname -s)/uid_*/job_*/; do
  [ -d "$d" ] || continue
  jid=$(basename "$d"); jid=${jid#job_}
  cpu=$(cat "${d}cpuacct.usage" 2>/dev/null || echo -1)
  memd=${d/cpuacct/memory}
  mem=$(cat "${memd}memory.usage_in_bytes" 2>/dev/null || echo -1)
  echo "JOB:${jid}:${cpu}:${mem}"
done
