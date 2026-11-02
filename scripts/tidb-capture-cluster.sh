#!/usr/bin/env bash
# Start, stop or inspect the instrumented TiDB capture cluster used by the TiDB capture PoC.
# Topology and paths come from the environment; every host must already hold RUN/bin from the build.
set -euo pipefail
usage() { echo "usage: $0 start TAG | stop | status" >&2; exit 2; }
[ "$#" -ge 1 ] || usage
: "${RUN:?run directory on every host}" "${PD_HOST:?}" "${TIKV_HOSTS:?space-separated}" "${TIDB_HOSTS:?space-separated}"
SSH=${SSH:-ssh}
ip_of() { $SSH "$1" "hostname -I | awk '{print \$1}'"; }
# Detach the daemon's shell from the SSH channel so the call returns.
as_sibench() { $SSH -n "$1" "runuser -u sibench -- setsid bash -c $(printf %q "$2") >/dev/null 2>&1 </dev/null"; }

start() {
  local tag=$1 pd_ip i=0
  pd_ip=$(ip_of "$PD_HOST")
  as_sibench "$PD_HOST" "cd $RUN && mkdir -p data/pd logs && nohup bin/pd-server --name=pd1 --data-dir=data/pd \
    --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://$pd_ip:2379 \
    --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://$pd_ip:2380 \
    --initial-cluster=pd1=http://$pd_ip:2380 --log-file=logs/pd.log >logs/pd.out 2>&1 </dev/null &"
  for h in $TIKV_HOSTS; do
    local ip; ip=$(ip_of "$h")
    # SIBENCH_TIKV_CAPTURE is empty for an uninstrumented-cost run of the same binary.
    as_sibench "$h" "cd $RUN && mkdir -p data/tikv logs capture && SIBENCH_TIKV_CAPTURE=${CAPTURE:+$RUN/capture/tikv-$tag.jsonl} \
      nohup bin/${TIKV_BINARY:-tikv-server-capture} --pd=$pd_ip:2379 --addr=0.0.0.0:20160 --advertise-addr=$ip:20160 \
      --status-addr=0.0.0.0:20180 --advertise-status-addr=$ip:20180 --data-dir=data/tikv \
      --config=conf/tikv.toml --log-file=logs/tikv-$tag.log >logs/tikv-$tag.out 2>&1 </dev/null &"
  done
  for h in $TIDB_HOSTS; do
    i=$((i + 1))
    as_sibench "$h" "cd $RUN && mkdir -p logs capture && SIBENCH_TIDB_CAPTURE=${CAPTURE:+$RUN/capture/tidb$i-$tag.jsonl} \
      SIBENCH_TIDB_TASK_BASE=$((i << 48)) nohup bin/tidb-server --store=tikv --path=$pd_ip:2379 -P 4000 --status=10080 \
      --config=conf/tidb.toml --log-file=logs/tidb-$tag.log >logs/tidb-$tag.out 2>&1 </dev/null &"
  done
  for h in $TIDB_HOSTS; do
    for _ in $(seq 120); do
      if $SSH "$h" "curl -sf http://127.0.0.1:10080/status >/dev/null"; then continue 2; fi
      sleep 1
    done
    echo "TiDB on $h did not become ready" >&2; exit 1
  done
}

wait_gone() {
  for h in "$@"; do
    for _ in $(seq 120); do
      $SSH "$h" "pgrep -u sibench -f 'bin/(tidb|tikv|pd)-server' >/dev/null" || continue 2
      sleep 1
    done
    echo "processes remain on $h" >&2; exit 1
  done
}

# Stop by tier: TiKV needs PD while it shuts down.
stop() {
  for h in $TIDB_HOSTS; do $SSH "$h" "pkill -u sibench -x tidb-server || true"; done
  for h in $TIKV_HOSTS; do $SSH "$h" "pkill -u sibench -f 'bin/tikv-server' || true"; done
  wait_gone $TIKV_HOSTS
  $SSH "$PD_HOST" "pkill -u sibench -x pd-server || true"
  wait_gone $PD_HOST $TIDB_HOSTS
}

case "$1" in
  start) [ "$#" -eq 2 ] || usage; start "$2" ;;
  stop) stop ;;
  status) for h in $PD_HOST $TIKV_HOSTS $TIDB_HOSTS; do echo "$h: $($SSH "$h" "pgrep -a -u sibench -f 'bin/(tidb|tikv|pd)-server' | cut -c1-80")"; done ;;
  *) usage ;;
esac
