"""Combine only the two audited public diffs in a new, disposable checkout."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

BASELINE = 'eaa4128681bc53487138a4bbc22d58336ebe70d2'
UTC = '83ded3a6e027be978c78cecc68c920e278e98de1'
TEMPORAL = '1e6289c1ee40c42dc588791217e3ee0efc175826'
ALLOWED = {
    'utc': {'graphiti_core/helpers.py', 'tests/test_db_date_roundtrip.py'},
    'temporal': {
        'graphiti_core/utils/maintenance/edge_operations.py',
        'tests/utils/maintenance/test_edge_temporal_identity.py',
        'tests/utils/maintenance/test_edge_temporal_resolution.py',
    },
}
TRUSTED_LOCAL_PATHS = []


def git_prefix():
    return ['git', *[arg for p in TRUSTED_LOCAL_PATHS for arg in
                    ('-c', f'safe.directory={p.as_posix()}')]]


def git(directory, *args):
    # Scope trust to these explicitly supplied local checkouts, not global config.
    return subprocess.check_output(
        [*git_prefix(), '-c', f'safe.directory={directory.as_posix()}', '-C', str(directory), *args]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'utc', 'temporal', 'candidate', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    paths = {name: getattr(args, name).resolve() for name in vars(args)}
    for name in ('baseline', 'utc', 'temporal'):
        TRUSTED_LOCAL_PATHS.append(paths[name])
        dotgit = paths[name] / '.git'
        if dotgit.is_file():
            target = dotgit.read_text().strip().removeprefix('gitdir: ')
            TRUSTED_LOCAL_PATHS.append((paths[name] / target).resolve())
        else:
            TRUSTED_LOCAL_PATHS.append(dotgit.resolve())
    output, candidate = paths['output'], paths['candidate']
    output.mkdir(parents=True, exist_ok=True)
    if candidate.exists():
        raise RuntimeError('Candidate path must be new; no existing directory is removed')
    pins = {'baseline': BASELINE, 'utc': UTC, 'temporal': TEMPORAL}
    for name, pin in pins.items():
        actual = git(paths[name], 'rev-parse', 'HEAD').decode().strip()
        if actual != pin:
            raise RuntimeError(f'{name}: expected {pin}, got {actual}')
        if git(paths[name], 'status', '--porcelain').strip():
            raise RuntimeError(f'{name}: checkout must be clean')
    subprocess.run([
        *git_prefix(),
        'clone', '--no-checkout', '--no-hardlinks', str(paths['baseline']), str(candidate),
    ], check=True)
    git(candidate, 'config', 'core.autocrlf', 'false')
    git(candidate, 'checkout', '--detach', BASELINE)
    manifests = {}
    for name in ('utc', 'temporal'):
        git(candidate, 'fetch', '--no-tags', str(paths[name]), pins[name])
        changed = set(git(candidate, 'diff', '--name-only', BASELINE, pins[name]).decode().splitlines())
        if changed != ALLOWED[name]:
            raise RuntimeError(f'{name}: unexpected changed paths: {sorted(changed)}')
        patch = git(candidate, 'diff', '--binary', BASELINE, pins[name])
        patch_path = output / f'{name}.patch'
        patch_path.write_bytes(patch)
        git(candidate, 'apply', '--check', str(patch_path))
        git(candidate, 'apply', str(patch_path))
        # Verify the applied file bytes, not only a patch exit status.
        for relative in changed:
            expected = git(candidate, 'show', f'{pins[name]}:{relative}')
            if (candidate / relative).read_bytes() != expected:
                raise RuntimeError(f'{name}: applied bytes differ from public blob: {relative}')
        manifests[name] = {
            'commit': pins[name], 'changed_paths': sorted(changed),
            'patch_sha256': hashlib.sha256(patch).hexdigest(),
        }
    # Newly added untracked test files are absent from ordinary git diff.
    git(candidate, 'add', '--intent-to-add', *sorted(ALLOWED['utc'] | ALLOWED['temporal']))
    combined = git(candidate, 'diff', '--binary', BASELINE)
    (output / 'combined.patch').write_bytes(combined)
    manifest = {
        'baseline_commit': BASELINE, 'public_patches': manifests,
        'candidate_base_commit': git(candidate, 'rev-parse', 'HEAD').decode().strip(),
        'candidate_is_local_combination_not_a_public_commit': True,
        'candidate_source_sha256': {
            p: hashlib.sha256((candidate / p).read_bytes()).hexdigest()
            for p in sorted(ALLOWED['utc'] | ALLOWED['temporal'])
        },
        'combined_patch_sha256': hashlib.sha256(combined).hexdigest(),
        'uv_lock_sha256': hashlib.sha256((candidate / 'uv.lock').read_bytes()).hexdigest(),
    }
    (output / 'source-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
