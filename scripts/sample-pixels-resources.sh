#!/usr/bin/env bash
# Append one resource sample per INTERVAL seconds until STOP_FILE exists: pod memory, /data free space,
# and summed RSS of matching processes on the host. Usage: sample-pixels-resources.sh OUT STOP_FILE [INTERVAL]
# v1 total_rss includes descendants; it is not the sum of process RSS (which double-counts shared pages).
sample_memory() {
  local root=$1 mem anon metric
  if [[ -f "$root/memory.current" ]]; then
    mem=$(cat "$root/memory.current" 2>/dev/null)
    anon=$(awk '$1=="anon"{print $2}' "$root/memory.stat" 2>/dev/null)
    metric=cgroup_v2_anon
  else
    mem=$(cat "$root/memory/memory.usage_in_bytes" 2>/dev/null)
    anon=$(awk '$1=="total_rss"{print $2}' "$root/memory/memory.stat" 2>/dev/null)
    metric=cgroup_v1_total_rss
  fi
  [[ "$mem" =~ ^[0-9]+$ ]] || mem=null
  [[ "$anon" =~ ^[0-9]+$ ]] || { anon=null; metric=unavailable; }
  printf '"pod_memory":%s,"pod_anon":%s,"pod_anon_metric":"%s"' "$mem" "$anon" "$metric"
}

[[ "${BASH_SOURCE[0]}" == "$0" ]] || return 0
[[ $# -ge 2 && $# -le 3 ]] || { echo "usage: $0 OUT STOP_FILE [INTERVAL]" >&2; exit 2; }
OUT=$1; STOP=$2; INTERVAL=${3:-60}
[[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]] || exit 2
while [ ! -e "$STOP" ]; do
  memory=$(sample_memory /sys/fs/cgroup)
  free=$(df -B1 --output=avail /data | tail -1 | tr -d ' ')
  [[ "$free" =~ ^[0-9]+$ ]] || free=null
  procs=$(ps -eo rss=,args= | awk '/PixelsRetina|PixelsCoordinator|PixelsSinkApp|etcd --data|sibench.jar|postgres|PixelsSetup|com.hybench|org.sibench.capture.Hybench/ && !/awk/ {
    n="other"; if(/PixelsRetina/)n="retina"; else if(/PixelsCoordinator/)n="coordinator"; else if(/PixelsSinkApp/)n="sink";
    else if(/etcd/)n="etcd"; else if(/PixelsSetup/)n="setup"; else if(/Hybench/)n="hybench"; else if(/sibench.jar/)n="sibench"; else if(/postgres/)n="postgres";
    s[n]+=$1*1024} END{for(k in s)printf "\"%s\":%d,",k,s[k]}')
  wal=$(du -sb "${WAL_DIR:-/nonexistent}" 2>/dev/null | cut -f1)
  echo "{\"t\":$(date +%s),$memory,\"data_free\":$free,\"wal_bytes\":${wal:-null},\"rss\":{${procs%,}}}" >> "$OUT"
  sleep "$INTERVAL"
done
