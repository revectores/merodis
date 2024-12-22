import platform
import json
import subprocess
import inflection
from pathlib import Path
subprocess.run(["ls", "-l"])

result_dir = 'bench_results'

cases = [
    'FixedGetSmall',
    'FixedGetLarge',
    'FixedSetSmall',
    'FixedSetLarge',
    'RandomGet',
    'RandomSet',
    'LIndex',
    'LRange',
    'LInsert',
    'LRemSingle',
    'LRemPair',
    'LRemPoints',
    'LRemFragments',
    'SAdd',
    'SRem',
    'SIsMember',
    'ZRank',
]

if not Path('merodis_benchmark').is_file():
    print("benchmark executable does not exist")
    exit()

for case in cases:
    assert case
    result_filename = f'{inflection.underscore(case[0:2].lower() + case[2:])}.json'
    print(result_filename)
    result_path = Path(result_dir) / result_filename
    if result_path.is_file():
        print(f'{result_path} exists, skip')
        continue
    print(f'running bench {case}...')
    options = f'--benchmark_filter={case} --benchmark_format=json'
    resp = subprocess.check_output(f'./merodis_benchmark {options}', shell=True)
    json.dump(json.loads(resp)['benchmarks'], open(result_path, 'w'))
    print(f'bench {case} results dumped completed!')
