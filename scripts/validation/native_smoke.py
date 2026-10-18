#!/usr/bin/env python3
"""Run native benchmark cases against an existing isolated PostgreSQL instance.
Builds must exist in build/benchmarks; this is a verification script, not a trace collector.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
BENCHMARKS = ROOT / 'benchmark'
BUILD = ROOT / 'build/benchmarks'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--benchmark', required=True, choices=['ycsb', 'sysbench', 'hybench'])
    ap.add_argument('--port', type=int, default=55432)
    ap.add_argument('--psql', default=str(ROOT / 'build/services/postgres/runtime/usr/lib/postgresql/14/bin/psql'))
    ap.add_argument('--java-home', default='/usr/lib/jvm/java-17-openjdk-amd64')
    args = ap.parse_args()
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S-%f')
    out = ROOT / 'results/native-validation' / f'{args.benchmark}-{stamp}'
    out.mkdir(parents=True)
    env = os.environ.copy()
    env['JAVA_HOME'] = args.java_home
    env['PATH'] = args.java_home + '/bin:' + env['PATH']
    env['PGCONNECT_TIMEOUT'] = '5'
    steps = []
    db = f'sibench_{args.benchmark}'
    psql = [args.psql, '-X', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', str(args.port), '-U', 'postgres']

    def run(name, cmd, cwd=ROOT, timeout=120):
        log = out / f'{name}.txt'
        with log.open('w') as f:
            result = subprocess.run(list(map(str, cmd)), cwd=cwd, env=env, stdout=f,
                                    stderr=subprocess.STDOUT, timeout=timeout)
        steps.append({'name': name, 'argv': list(map(str, cmd)), 'cwd': str(cwd), 'exit_code': result.returncode})
        if result.returncode:
            raise RuntimeError(f'{name} failed: {log}')
        return log.read_text()

    def sql(name, statement):
        return run(name, psql + ['-d', db, '-At', '-c', statement])

    metadata = {'benchmark': args.benchmark, 'started_at': stamp, 'port': args.port, 'steps': steps,
                'scope': 'native smoke; no trace capture or index server replay'}
    try:
        # CREATE fails if the name already exists: never overwrite another experiment.
        run('create-database', psql + ['-d', 'postgres', '-c', f'CREATE DATABASE {db}'])
        template = ROOT / 'configs/workloads' / args.benchmark
        name = {'ycsb': 'ycsb.properties', 'sysbench': 'sysbench.conf', 'hybench': 'hybench.props'}[args.benchmark]
        config = out / name
        config.write_text((template / ('smoke' + Path(name).suffix + '.in')).read_text().replace('@PORT@', str(args.port)))
        if args.benchmark == 'ycsb':
            fields = ', '.join(f'FIELD{i} TEXT' for i in range(10))
            sql('ddl', f'CREATE TABLE usertable (YCSB_KEY VARCHAR(255) COLLATE "C" PRIMARY KEY, {fields})')
            bench = BUILD / 'YCSB'
            dist = bench/'jdbc/target/ycsb-jdbc-binding-0.18.0-SNAPSHOT'
            cp = ':'.join(map(str, [dist/'lib/*', dist/'jdbc-binding/lib/*', BENCHMARKS/'HyBench-2024/lib/postgresql-42.5.0.jar']))
            command = [Path(args.java_home)/'bin/java', '-cp', cp, 'site.ycsb.Client', '-db', 'site.ycsb.db.JdbcDBClient', '-P', config]
            load = run('load', command + ['-load'])
            assert '[INSERT], Return=OK, 1000' in load, load[-2000:]
            initial = sql('initial', 'SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable')
            output = run('run', command + ['-t'])
            assert '[READ], Return=OK,' in output and '[UPDATE], Return=OK,' in output
            assert not re.search(r'Return=(?!OK)\w+, [1-9]', output)
            assert sum(int(n) for n in re.findall(r'\[(?:READ|UPDATE)\], Operations, (\d+)', output)) == 200
            metadata['initial'] = initial.strip()
            metadata['final'] = sql('final', 'SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable').strip()
        elif args.benchmark == 'sysbench':
            bench = BUILD / 'sysbench'
            env['LUA_PATH'] = str(bench/'src/lua/?.lua') + ';;'
            # This revision overwrites Lua options loaded early by --config-file.
            # Preserve a config file as source of truth, but pass values as argv.
            options = [f'--{line}' for line in config.read_text().splitlines()
                       if line.strip() and not line.startswith('#')]
            command = [bench/'src/sysbench', *options, bench/'src/lua/oltp_read_write.lua']
            run('prepare', command + ['prepare'])
            metadata['initial'] = sql('initial', 'SELECT count(*), min(id), max(id) FROM sbtest1').strip()
            assert metadata['initial'] == '1000|1|1000'
            output = run('run', command + ['run'])
            assert re.search(r'transactions:\s+20\s', output)
            assert re.search(r'ignored errors:\s+0\s', output)
            metadata['final'] = sql('final', 'SELECT count(*), min(id), max(id) FROM sbtest1').strip()
            assert metadata['final'] == metadata['initial']
            run('cleanup', command + ['cleanup'])
            assert sql('cleanup-check', "SELECT count(*) FROM pg_tables WHERE schemaname='public'").strip() == '0'
        else:
            bench = BUILD / 'HyBench-2024'
            resources = out / 'resources'
            resources.mkdir()
            shutil.copy2(template/'parameters.toml', resources/'parameters.toml')
            (out/'conf').mkdir()
            # Remove an invalid upstream Log4j appender attribute in the runtime copy.
            logging = (bench/'conf/log4j2.properties').read_text().replace('appender.error.level = error\n', '')
            (out/'conf/log4j2.properties').write_text(logging)
            cp = ':'.join(map(str, [resources, bench/'target/classes', bench/'lib/*']))
            command = [Path(args.java_home)/'bin/java', '-cp', cp, 'com.hybench.HyBench', '-c', config]
            generated = run('generate', command + ['-t', 'gendata'], cwd=out)
            assert 'Exception' not in generated and 'ERROR' not in generated
            run('ddl', psql + ['-d', db, '-f', bench/'conf/ddl_pg.sql'])
            load = (bench/'conf/load_data_pg.sql').read_text().replace('Data_1x/', 'Data_smoke/')
            (out/'load.sql').write_text(load)
            run('load', psql + ['-d', db, '-f', out/'load.sql'], cwd=out)
            run('indexes', psql + ['-d', db, '-f', bench/'conf/create_index_pg.sql'])
            tables = ['customer','company','savingaccount','checkingaccount','transfer','checking','loanapps','loantrans']
            counts = ' UNION ALL '.join(f"SELECT '{t}',count(*) FROM {t}" for t in tables)
            metadata['initial'] = sql('initial', counts).strip()
            expected = {'customer':1000,'company':10,'savingaccount':1010,'checkingaccount':1010,'transfer':10000,'checking':1000,'loanapps':1000,'loantrans':1000}
            assert dict((k,int(v)) for k,v in (line.split('|') for line in metadata['initial'].splitlines())) == expected
            output = run('run', command + ['-t', 'runtp', '-f', bench/'conf/stmt_postgres.toml'], cwd=out, timeout=100)
            assert 'Exception' not in output and 'ERROR' not in output, output[-3000:]
            metadata['final'] = sql('final', counts).strip()
            final = dict((k,int(v)) for k,v in (line.split('|') for line in metadata['final'].splitlines()))
            assert final['transfer'] > expected['transfer']
        run('drop-database', psql + ['-d', 'postgres', '-c', f'DROP DATABASE {db}'])
        metadata['status'] = 'passed'
    except Exception as exc:
        metadata['status'] = 'failed'
        metadata['error'] = str(exc)
        raise
    finally:
        (out/'result.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+'\n')
        print(out)


if __name__ == '__main__':
    main()
