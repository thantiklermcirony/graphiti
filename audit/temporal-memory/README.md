# Complete the Graphiti Neo4j validation on Linux

This is a **prepared, not yet executed Linux audit**. It closes one specific evidence gap if its future run passes: the full-text-dependent batch case that was skipped on the Windows sandbox. It also reruns the earlier storage cases, finite regressions and unchanged UTC roundtrip. It is separate from the two pull-request branches and changes no Graphiti production code.

## Files and placement

On the user's fork `thantiklermcirony/graphiti`, create the audit-only branch `audit/temporal-memory`, without changing either fix branch. Upload all harness files first and the workflow last, so the first triggered run has every required file. Place these files as follows:

| Local file | Fork path |
|---|---|
| `temporal-memory-audit.yml` | `.github/workflows/temporal-memory-audit.yml` |
| `assemble_sources.py` | `audit/temporal-memory/assemble_sources.py` |
| `run_audit.py` | `audit/temporal-memory/run_audit.py` |
| `database_temporal_audit.py` | `audit/temporal-memory/database_temporal_audit.py` |
| `independent_temporal_cases.py` | `audit/temporal-memory/independent_temporal_cases.py` |
| This README | `audit/temporal-memory/README.md` |

Do not upload `local-validation/`, runtime environments, scratch checkouts or prior JSON as new results.

The workflow uses `contents: read`, receives no model secrets, and is restricted to the user's fork. Its push trigger is restricted to branch `audit/temporal-memory` and changes in the two audit paths above. Committing the workflow last triggers the initial run; no fork-default or PR-head changes are needed. Subsequent changes to these audit files trigger another bounded run.

Manual dispatch is optional. GitHub requires the dispatchable workflow to exist on the default branch before manual triggering is available; otherwise use the narrowly scoped push trigger or rerun an existing job. [GitHub manual-run documentation](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow)

If manual dispatch is registered, the equivalent owner CLI is:

```sh
gh workflow run temporal-memory-audit.yml --repo thantiklermcirony/graphiti --ref audit/temporal-memory
```

## Immutable inputs and dependencies

- Baseline: `getzep/graphiti@eaa4128681bc53487138a4bbc22d58336ebe70d2`.
- UTC-only source: `thantiklermcirony/graphiti@83ded3a6e027be978c78cecc68c920e278e98de1`.
- Temporal-only source: `thantiklermcirony/graphiti@1e6289c1ee40c42dc588791217e3ee0efc175826`.
- The assembler verifies the three HEADs and clean source checkouts, accepts exactly five expected changed paths across the two diffs, applies them to a disposable baseline clone, and verifies each applied file against its public Git blob. The combined candidate is explicitly a local combination, not an invented public commit. Original source trees are not edited.
- `uv==0.11.20` installs core dependencies with `uv sync --frozen --no-dev`; the same lockfile's dev export is used only as constraints for `pytest` and `pytest-asyncio`. Optional GPU/model packages are not installed. Environment versions and lock hashes are recorded. [uv lock/export documentation](https://docs.astral.sh/uv/concepts/projects/sync/), [pinned uv release](https://github.com/astral-sh/uv/releases/tag/0.11.20)
- Neo4j Community `5.26.30` is a disposable Linux service, no auth, host access restricted to `127.0.0.1:17687`. Container image ID and actual server/Java versions are retained because a version tag is not a cryptographic image pin. [Official Neo4j image](https://hub.docker.com/_/neo4j)

The job is capped at 20 minutes. The driver talks only to the dedicated loopback Bolt port. Model responses and four-dimensional vectors are synthetic fixtures. No paid model calls, API keys or production data are used; no graph backend is mocked. Model API calls remain explicitly zero in the result records.

## Exact meaning of pass and failure

A green run requires all of the following:

1. The three regression files run against the pristine baseline and combined candidate, with import paths asserted. Baseline reproduces 30 failures among 48 tests with no errors/skips; candidate passes all 48. The independent 16-method fixture suite fails on baseline and passes on candidate.
2. Both actual database runs execute **all four cases**, with required RANGE indexes and `edge_name_and_fact` FULLTEXT ONLINE. Every baseline case reproduces its known failure; every combined candidate case passes. A skipped batch case, connection error, unexpected model seam or missing result fails the verdict.
3. Separate unchanged save/read/save checks reproduce the baseline boundary error and pass on the public UTC-only source and combined candidate. Each uses exactly one real record, zero resolver calls and zero model calls; both temporal answers and native saved values remain stable in corrected runs.

The four storage cases cover reactivation after an explicit NOT_ASSIGNED interval, clipping older finite intervals, no-context expiration metadata, and retaining two disjoint periods through the **real full batch resolver, search and persistence**. Exact-boundary checks remain in the first three cases. No-context valid-time queries already worked on baseline; that case's original failure is missing expiration metadata.

Success establishes this bounded resolver/storage integration on the pinned Linux/Neo4j combination. It does **not** establish semantic extraction, negation understanding, retrieval relevance, original issue 1841 end-to-end resolution, knowledge-time replay, a learned architecture, market impact, broad performance superiority or all of Graphiti's service behavior.

On failure, inspect `verdict.json`, per-stage logs/JSON, JUnit XML and container logs before changing anything. Baseline assertion failures are expected only when the JSON demonstrates the intended failure. A baseline crash, empty result, collection error or skipped case is not accepted as reproduction. Keep failed artifacts; do not weaken the oracle or relabel skipped checks as passing.

## Local Linux commands

Prepare clean clones at the three exact commit IDs above. With Python 3.12, uv 0.11.20 and Docker installed, run the same assembly, locked install and `run_audit.py` commands shown in the workflow. Start a fresh dedicated service before the audit:

```sh
docker run --rm -d --name graphiti-temporal-audit \
  -p 127.0.0.1:17687:7687 -e NEO4J_AUTH=none \
  -e NEO4J_server_memory_heap_max__size=512m neo4j:5.26.30-community
```

Wait for `docker exec graphiti-temporal-audit cypher-shell 'RETURN 1'` to succeed. The harness creates and waits for its real indexes. Stop only this named disposable container when finished. Retain `evidence/` before removing temporary checkouts.

## Portability changes from the earlier harness

The supplied-decision protocol and stored/query expectations are retained. The copy asserts its actual imported source; waits up to 60 seconds for a POPULATING FULLTEXT index; exposes `--require-fulltext` so skipped cases return a failing status; creates output parent directories; and rejects empty roundtrip results while checking exact equality and native-value stability. The orchestrator explicitly rejects partial database reports. None of these changes replaces or fakes the database.
