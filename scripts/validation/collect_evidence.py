#!/usr/bin/env python3
"""Snapshot the latest validated native benchmark runs into the repository report."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

root = Path(__file__).resolve().parents[2]
benchmarks = root / 'benchmark'
results = root / 'results/native-validation'
stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S-%f')
evidence = root / 'docs/validation/evidence' / f'{stamp}-native-smoke'
evidence.mkdir(parents=True, exist_ok=False)
summary = {'date': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
           'scope': 'native PostgreSQL benchmark smoke, not performance comparison',
           'source_commits': {}, 'runs': {}, 'environment': {},
           'limitations': ['No index server/object storage execution',
                           'HyBench Maven dependency graph blocked; javac path validated',
                           'Derived workloads; no full standard-suite validation',
                           'First sysbench config-file attempt loaded 10000 rather than 1000; excluded']}
for name, sub in [('ycsb','YCSB'),('sysbench','sysbench'),('hybench','HyBench-2024')]:
    summary['source_commits'][name] = subprocess.check_output(['git','-C',str(benchmarks/sub),'rev-parse','HEAD'],text=True).strip()
    candidates = []
    for path in sorted(results.glob(f'{name}-*/result.json')):
        data = json.loads(path.read_text())
        if data['status'] != 'passed':
            continue
        if name == 'sysbench' and data.get('initial') != '1000|1|1000':
            continue
        candidates.append((path,data))
    if not candidates:
        raise RuntimeError(f'No validated run: {name}')
    path,data = candidates[-1]
    target=evidence/name
    target.mkdir(exist_ok=True)
    for file in path.parent.iterdir():
        if file.is_file() and file.suffix in ('.txt','.json','.properties','.props','.conf','.sql'):
            shutil.copy2(file,target/file.name)
    summary['runs'][name]={'run_directory':str(path.parent.relative_to(root)),
                           'status':'passed','initial':data['initial'],'final':data['final']}
    (target/'result.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
for name in ['ycsb-build.log','sysbench-build.log','hybench-build.log','hybench-javac.log','postgres-init.log','source-versions.txt']:
    if (results/name).exists():
        shutil.copy2(results/name,evidence/(name.replace('.log','.txt')))
commands={'java':['/usr/lib/jvm/java-17-openjdk-amd64/bin/java','-version'],
          'postgres':[str(root/'build/services/postgres/runtime/usr/lib/postgresql/14/bin/postgres'),'--version'],
          'sysbench':[str(root/'build/benchmarks/sysbench/src/sysbench'),'--version'],
          'maven':['mvn','-version']}
for name,cmd in commands.items():
    summary['environment'][name]=subprocess.check_output(cmd,stderr=subprocess.STDOUT,text=True).strip()
summary['environment']['maven_note']='mvn -version reflects shell default Java; builds use JAVA_HOME=Java17 explicitly.'
(evidence/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
# Include source artifacts and binaries to tie outcomes to configuration/code.
files=list(evidence.rglob('*'))+list((root/'config').rglob('*'))+list((root/'scripts').rglob('*.py'))+list((root/'scripts').rglob('*.sh'))
files += list((root/'src').rglob('*.java')) + [root/'pom.xml']
files += [root/'build/benchmarks/sysbench/src/sysbench',root/'build/benchmarks/YCSB/core/target/core-0.18.0-SNAPSHOT.jar',root/'build/benchmarks/YCSB/jdbc/target/jdbc-binding-0.18.0-SNAPSHOT.jar']
files += list((root/'build/benchmarks/HyBench-2024/target/classes').rglob('*.class'))
lines=[]
for file in sorted(files):
    if file.is_file() and file.name!='SHA256SUMS':
        lines.append(hashlib.sha256(file.read_bytes()).hexdigest()+'  '+str(file.relative_to(root)))
(evidence/'SHA256SUMS').write_text('\n'.join(lines)+'\n')
print(evidence)
