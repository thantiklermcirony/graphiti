"""Run all stages and reject missing cases, unexpected crashes or wrong-source imports."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
REGRESSIONS = [
    'tests/utils/maintenance/test_edge_temporal_identity.py',
    'tests/utils/maintenance/test_edge_temporal_resolution.py',
    'tests/test_db_date_roundtrip.py',
]
CASES = {'reactivation', 'finite_interval_clipping', 'no_context_expiry', 'batch_period_retention'}


def assess_database(report, expected_pass):
    errors = []
    by_name = {c['case']: c for c in report.get('cases', [])}
    if set(by_name) != CASES or len(report.get('cases', [])) != len(CASES):
        errors.append('Case set is incomplete or unexpected')
    if report.get('fatal_error'):
        errors.append('Database harness fatal error')
    if not report.get('required_fulltext_index_online'):
        errors.append('Required FULLTEXT index is not ONLINE')
    if not report.get('required_range_indexes_online'):
        errors.append('Required RANGE indexes are not ONLINE')
    if report.get('model_api_calls') != 0:
        errors.append('External model call count must be zero')
    for name, case in by_name.items():
        if case.get('status') != 'executed':
            errors.append(f'{name}: not executed successfully')
        if case.get('pass') is not expected_pass:
            errors.append(f'{name}: expected pass={expected_pass}')
        if not case.get('temporal_queries') or not case.get('storage_assertions'):
            errors.append(f'{name}: missing actual query/storage assertions')
    return errors


def assess_roundtrip(report, expected_pass):
    errors = []
    if report.get('fatal_error'):
        errors.append('Roundtrip harness fatal error')
    if report.get('unchanged_temporal_answers') is not expected_pass:
        errors.append(f'Expected unchanged_temporal_answers={expected_pass}')
    if report.get('model_api_calls') != 0 or report.get('resolver_calls') != 0:
        errors.append('Roundtrip must call neither models nor resolver')
    for key in ('after_initial_save_database_values', 'after_unchanged_resave_database_values'):
        if len(report.get(key, [])) != 1:
            errors.append(f'{key}: expected one actual stored record')
    if expected_pass and report.get('unchanged_native_values') is not True:
        errors.append('Candidate unchanged roundtrip altered native values')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'candidate', 'utc', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    paths = {name: getattr(args, name).resolve() for name in vars(args)}
    output = paths['output']
    output.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()
    results, problems = [], []
    env_base = os.environ.copy()
    env_base.update({'GRAPHITI_TELEMETRY_ENABLED': 'false', 'PYTHONDONTWRITEBYTECODE': '1'})
    for backend in ('NEO4J', 'FALKORDB', 'KUZU', 'NEPTUNE'):
        env_base[f'DISABLE_{backend}'] = '1'
    # No secrets are supplied by the workflow; discard common API keys if run locally.
    for key in list(env_base):
        if key.endswith('_API_KEY') or key in ('OPENAI_TOKEN', 'ANTHROPIC_AUTH_TOKEN'):
            env_base.pop(key)

    def run(name, source, command, timeout=180):
        env = env_base | {'GRAPHITI_TEST_SOURCE': str(source), 'PYTHONPATH': str(source)}
        begin = time.monotonic()
        try:
            process = subprocess.run(command, cwd=source, env=env, timeout=timeout,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='replace')
            text, code = process.stdout, process.returncode
        except subprocess.TimeoutExpired as exc:
            text = f'TIMEOUT after {timeout}s\n{exc.stdout!r}'
            code = 124
        (output / f'{name}.log').write_text(text, encoding='utf-8')
        result = {'name': name, 'exit_code': code, 'source': str(source),
                  'command': command, 'elapsed_seconds': round(time.monotonic()-begin, 3)}
        results.append(result)
        print(json.dumps(result), flush=True)
        return code

    guard = (
        'import sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);'
        'import graphiti_core.helpers as h;'
        'import graphiti_core.utils.maintenance.edge_operations as o;'
        'print("ACTUAL_HELPERS="+h.__file__,flush=True);'
        'print("ACTUAL_RESOLVER="+o.__file__,flush=True);'
        'assert Path(h.__file__).resolve().is_relative_to(Path(sys.argv[1]));'
        'assert Path(o.__file__).resolve().is_relative_to(Path(sys.argv[1]));'
        'import pytest;raise SystemExit(pytest.main(sys.argv[2:]))'
    )
    for label in ('baseline', 'candidate'):
        xml = output / f'{label}-regressions.xml'
        command = [sys.executable, '-B', '-c', guard, str(paths[label]),
                   *[str(paths['candidate'] / p) for p in REGRESSIONS], '-q', '--tb=short',
                   '-p', 'no:cacheprovider', '--junitxml=' + str(xml),
                   '--basetemp=' + str(output / f'{label}-pytest-tmp')]
        code = run(f'{label}-regressions', paths[label], command)
        if not xml.exists():
            problems.append(f'{label} regressions produced no JUnit evidence')
        else:
            root = ET.parse(xml).getroot()
            tests = root.findall('.//testcase')
            failures = sum(t.find('failure') is not None for t in tests)
            errors = sum(t.find('error') is not None for t in tests)
            skipped = sum(t.find('skipped') is not None for t in tests)
            results[-1]['counts'] = {'tests': len(tests), 'failures': failures,
                                    'errors': errors, 'skipped': skipped}
            if len(tests) != 48 or errors or skipped:
                problems.append(f'{label} regressions have missing/unexpected test outcomes')
            if label == 'baseline' and (code != 1 or failures != 30):
                problems.append('Baseline must reproduce the 30 known regression failures')
            if label == 'candidate' and (code != 0 or failures):
                problems.append('Candidate regression suite failed')
        code = run(f'{label}-independent', paths[label],
            [sys.executable, '-B', str(HERE / 'independent_temporal_cases.py'), '-v'])
        independent_log = (output / f'{label}-independent.log').read_text()
        valid_summary = re.search(r'^Ran 16 tests in ', independent_log, re.MULTILINE)
        valid_summary = valid_summary and (
            'FAILED (failures=11)' in independent_log if label == 'baseline'
            else re.search(r'^OK$', independent_log, re.MULTILINE)
        )
        if not valid_summary:
            problems.append(f'{label} independent suite has missing/unexpected test summary')
        if code != (1 if label == 'baseline' else 0):
            problems.append(f'{label} independent suite returned unexpected exit code {code}')

    for label in ('baseline', 'candidate'):
        name = f'{label}-database'
        report_path = output / f'{name}.json'
        code = run(name, paths[label], [sys.executable, '-B', str(HERE / 'database_temporal_audit.py'),
            '--stage', name, '--require-fulltext', '--output', str(report_path)])
        if not report_path.exists():
            problems.append(f'{name}: missing JSON report')
        else:
            report = json.loads(report_path.read_text())
            problems.extend(f'{name}: {p}' for p in assess_database(report, label == 'candidate'))
            if code != (1 if label == 'baseline' else 0):
                problems.append(f'{name}: unexpected process exit {code}')

    # The UTC-only public source is evaluated independently of temporal changes.
    for label in ('baseline', 'utc', 'candidate'):
        name = f'{label}-roundtrip'
        report_path = output / f'{name}.json'
        code = run(name, paths[label], [sys.executable, '-B', str(HERE / 'database_temporal_audit.py'),
            '--stage', name, '--roundtrip-only', '--output', str(report_path)])
        if not report_path.exists():
            problems.append(f'{name}: missing JSON report')
        else:
            report = json.loads(report_path.read_text())
            problems.extend(f'{name}: {p}' for p in assess_roundtrip(report, label != 'baseline'))
            if code != (1 if label == 'baseline' else 0):
                problems.append(f'{name}: unexpected process exit {code}')

    final = {'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
             'pass': not problems, 'problems': problems, 'stages': results,
             'meaning': 'Finite deterministic bookkeeping and real persistence integration; '
                        'supplied semantic decisions and constant vectors, no model-quality claim.'}
    (output / 'verdict.json').write_text(json.dumps(final, indent=2) + '\n')
    summary = '## Graphiti temporal audit\n\n' + ('PASS' if not problems else 'FAIL') + '\n\n'
    summary += '\n'.join('- ' + p for p in problems) if problems else (
        'All four database cases executed; baseline failures reproduced; combined candidate passed. '
        'UTC-only and combined unchanged roundtrips passed. No model API calls.\n')
    summary += '\n\nSee JSON/log artifacts for exact evidence and source provenance. '
    summary += 'This does not establish semantic extraction, retrieval quality or full service correctness.\n'
    (output / 'SUMMARY.md').write_text(summary)
    print(summary)
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as f:
            f.write(summary)
    return 0 if not problems else 1


if __name__ == '__main__':
    raise SystemExit(main())
