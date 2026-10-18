#!/usr/bin/env bash
# Dedicated local PostgreSQL; no changes to a system cluster.
set -euo pipefail
sibench_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$sibench_root"
sibench_pg_bin=${SIBENCH_PG_BIN:-$sibench_root/build/services/postgres/runtime/usr/lib/postgresql/14/bin}
sibench_pg_share=${SIBENCH_PG_SHARE:-$sibench_root/build/services/postgres/runtime/usr/share/postgresql/14}
sibench_pg_data="$sibench_root/build/services/postgres/data"
sibench_pg_port=${SIBENCH_PG_PORT:-55432}
mkdir -p build/services/postgres results/native-validation
case "${1:-}" in
    fetch)
        mkdir -p build/services/postgres/packages
        (cd build/services/postgres/packages && apt-get download postgresql-14 postgresql-client-14)
        for package in build/services/postgres/packages/postgresql*.deb; do
            dpkg-deb -x "$package" build/services/postgres/runtime
        done
        ;;
    start)
        if [[ ! -e "$sibench_pg_data" ]]; then
            "$sibench_pg_bin/initdb" -D "$sibench_pg_data" -L "$sibench_pg_share" \
                --no-locale --encoding=UTF8 --auth-local=trust --auth-host=trust -U postgres \
                > results/native-validation/postgres-init.log 2>&1
        fi
        "$sibench_pg_bin/pg_ctl" -D "$sibench_pg_data" \
            -l "$sibench_root/results/native-validation/postgres-server.log" \
            -o "-p $sibench_pg_port -h 127.0.0.1 -k $sibench_pg_data" -w start
        ;;
    stop) "$sibench_pg_bin/pg_ctl" -D "$sibench_pg_data" -m fast -w stop ;;
    *) echo 'Usage: bash scripts/services/postgres.sh fetch|start|stop' >&2; exit 2 ;;
esac
