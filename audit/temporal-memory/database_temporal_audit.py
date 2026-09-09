"""Real Neo4j storage/temporal-query audit with explicitly supplied model decisions.

Requires the dedicated synthetic-data server at bolt://127.0.0.1:17687.
Select real source using GRAPHITI_TEST_SOURCE. No graph backend is mocked.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import sys
import time
import traceback
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

os.environ['GRAPHITI_TELEMETRY_ENABLED'] = 'false'
os.environ['EMBEDDING_DIM'] = '4'
SOURCE = Path(os.environ.get(
    'GRAPHITI_TEST_SOURCE', str(Path(__file__).resolve().parent / 'graphiti')
)).resolve()
sys.path.insert(0, str(SOURCE))

from graphiti_core.cross_encoder.client import CrossEncoderClient
from graphiti_core.driver.neo4j_driver import Neo4jDriver
from graphiti_core.edges import EntityEdge
from graphiti_core.embedder.client import EmbedderClient
from graphiti_core.graphiti_types import GraphitiClients
from graphiti_core.llm_client import LLMClient
from graphiti_core.nodes import EntityNode, EpisodicNode
from graphiti_core.search.search_filters import ComparisonOperator, DateFilter, SearchFilters
from graphiti_core.tracer import NoOpTracer
from graphiti_core.utils.maintenance import edge_operations as ops

if not Path(ops.__file__).resolve().is_relative_to(SOURCE):
    raise RuntimeError(f'Imported resolver is outside selected source: {ops.__file__}')

PIN = 'eaa4128681bc53487138a4bbc22d58336ebe70d2'
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
VECTOR = [1.0, 0.0, 0.0, 0.0]
FACT = 'Kiran is assigned to the Payments Project.'

# Written independently of resolver output. These are valid-time answers after
# all supplied events have been ingested, not historical knowledge-time queries.
EXPECTED = {
    'reactivation': [
        (5, ['ASSIGNED']), (10, ['NOT_ASSIGNED']), (15, ['NOT_ASSIGNED']),
        (20, ['ASSIGNED']), (25, ['ASSIGNED']),
    ],
    'finite_interval_clipping': [
        (5, ['ASSIGNED']), (10, ['NOT_ASSIGNED']), (15, ['NOT_ASSIGNED']),
        (25, ['NOT_ASSIGNED']),
    ],
    'no_context_expiry': [(-1, []), (5, ['ASSIGNED']), (10, []), (15, [])],
    'batch_period_retention': [(2, ['ASSIGNED']), (7, []), (12, ['ASSIGNED'])],
}


def moment(day):
    return BASE + timedelta(days=day)


def json_value(value):
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if hasattr(value, 'iso_format'):
        return value.iso_format()
    raise TypeError(f'Unsupported JSON value: {type(value)!r}')


def snapshot(edge):
    result = edge.model_dump(mode='json')
    result.pop('fact_embedding', None)
    return result


class AuditedNeo4jDriver(Neo4jDriver):
    """Count forwarded real queries; never change query text, results or backend."""
    def __init__(self, uri):
        self.phase = 'setup'
        self.calls = Counter()
        self.query_hashes = Counter()
        super().__init__(uri=uri, user=None, password=None, database='neo4j')

    async def execute_query(self, cypher_query_, **kwargs):
        self.calls[self.phase] += 1
        self.query_hashes[hashlib.sha256(cypher_query_.encode()).hexdigest()] += 1
        return await super().execute_query(cypher_query_, **kwargs)


class SuppliedDecisionClient(LLMClient):
    def __init__(self):
        super().__init__(config=None, cache=False)
        self.duplicates = []
        self.contradictions = []
        self.calls = []

    async def _generate_response(self, *args, **kwargs):
        raise AssertionError('No external or base model execution is permitted')

    async def generate_response(self, *args, **kwargs):
        name = kwargs.get('prompt_name')
        if name == 'dedupe_edges.resolve_edge':
            result = {
                'duplicate_facts': list(self.duplicates),
                'contradicted_facts': list(self.contradictions),
            }
        elif name == 'extract_edges.extract_timestamps':
            result = {'valid_at': None, 'invalid_at': None}
        else:
            raise AssertionError(f'Unexpected model seam: {name}')
        self.calls.append({'prompt_name': name, 'supplied_response': result})
        return result


class FixtureEmbedder(EmbedderClient):
    """Explicit constant synthetic vectors; no semantic-retrieval quality claim."""
    def __init__(self):
        self.calls = Counter()

    async def create(self, input_data):
        self.calls['create'] += 1
        return VECTOR.copy()

    async def create_batch(self, input_data_list):
        self.calls['create_batch'] += 1
        self.calls['batch_texts'] += len(input_data_list)
        return [VECTOR.copy() for _ in input_data_list]


class UnusedCrossEncoder(CrossEncoderClient):
    def __init__(self):
        self.calls = 0

    async def rank(self, query, passages):
        self.calls += 1
        raise AssertionError('RRF fixture should not invoke a cross-encoder')


class Fixture:
    def __init__(self, name, stage, run_id, driver):
        self.name = name
        self.group = f'ea_temporal_{stage}_{run_id}_{name}'
        self.driver = driver
        self.person = f'{self.group}_person'
        self.project = f'{self.group}_project'
        self.llm = SuppliedDecisionClient()
        self.embedder = FixtureEmbedder()
        self.reranker = UnusedCrossEncoder()
        self.nodes = []
        self.episodes = {}
        self.input_edges = []
        self.resolution_log = []

    async def initialize(self):
        for ident, name in ((self.person, 'Kiran'), (self.project, 'Payments Project')):
            node = EntityNode(
                uuid=ident, name=name, group_id=self.group, labels=[],
                name_embedding=VECTOR.copy(),
            )
            await node.save(self.driver)
            self.nodes.append(node)

    async def make_edge(
        self, ident, start, end=None, *, name='ASSIGNED', fact=FACT, expired=False
    ):
        ep = EpisodicNode(
            uuid=f'{self.group}_evidence_{ident}',
            name=f'Synthetic evidence {ident}',
            group_id=self.group,
            source='message',
            source_description='Controlled storage fixture; supplied facts and timing',
            content=fact,
            valid_at=moment(start) if start is not None else BASE,
        )
        await ep.save(self.driver)
        self.episodes[ident] = ep
        item = EntityEdge(
            uuid=f'{self.group}_{ident}',
            source_node_uuid=self.person,
            target_node_uuid=self.project,
            group_id=self.group,
            name=name,
            fact=fact,
            fact_embedding=VECTOR.copy(),
            episodes=[ep.uuid],
            created_at=datetime.now(timezone.utc),
            valid_at=moment(start) if start is not None else None,
            invalid_at=moment(end) if end is not None else None,
            expired_at=datetime.now(timezone.utc) if expired else None,
            reference_time=ep.valid_at,
        )
        self.input_edges.append(snapshot(item))
        return item

    async def save(self, *edges):
        seen = set()
        for item in edges:
            if item.uuid in seen:
                continue
            seen.add(item.uuid)
            # Default edge read APIs omit embeddings; restore the declared
            # synthetic vector before exercising the real vector-property save.
            if item.fact_embedding is None:
                item.fact_embedding = VECTOR.copy()
            await item.save(self.driver)

    async def load(self, ident):
        return await EntityEdge.get_by_uuid(self.driver, f'{self.group}_{ident}')

    async def resolve(self, incoming, related, broader, ep_ident, dup=(), contra=()):
        self.llm.duplicates = list(dup)
        self.llm.contradictions = list(contra)
        before_calls = len(self.llm.calls)
        result = await ops.resolve_extracted_edge(
            self.llm, incoming, related, broader, self.episodes[ep_ident], {}
        )
        resolved, invalidated, duplicates = result
        self.resolution_log.append({
            'input_uuid': incoming.uuid,
            'related_uuids': [item.uuid for item in related],
            'broader_uuids': [item.uuid for item in broader],
            'configured_duplicate_indices': list(dup),
            'configured_contradiction_indices': list(contra),
            'resolved': snapshot(resolved),
            'invalidated_uuids': [item.uuid for item in invalidated],
            'duplicate_uuids': [item.uuid for item in duplicates],
            'model_seam_calls': len(self.llm.calls) - before_calls,
        })
        await self.save(resolved, *invalidated)
        return resolved

    async def rows(self):
        records, _, _ = await self.driver.execute_query(
            """MATCH (s:Entity)-[e:RELATES_TO]->(t:Entity)
            WHERE e.group_id = $group AND s.group_id = $group AND t.group_id = $group
            RETURN e.uuid AS uuid, e.name AS name, e.fact AS fact,
                   e.valid_at AS valid_at, e.invalid_at AS invalid_at,
                   toString(e.valid_at) AS neo4j_valid_at_text,
                   toString(e.invalid_at) AS neo4j_invalid_at_text,
                   e.expired_at AS expired_at, e.created_at AS created_at,
                   e.episodes AS episodes, e.reference_time AS reference_time
            ORDER BY e.valid_at, e.uuid""",
            group=self.group,
            routing_='r',
        )
        return [dict(record) for record in records]

    async def query_answers(self):
        outcomes = []
        for day, expected in EXPECTED[self.name]:
            when = moment(day)
            records, _, _ = await self.driver.execute_query(
                """MATCH (s:Entity)-[e:RELATES_TO]->(t:Entity)
                WHERE e.group_id = $group AND s.group_id = $group AND t.group_id = $group
                  AND e.valid_at IS NOT NULL AND e.valid_at <= $asof
                  AND (e.invalid_at IS NULL OR $asof < e.invalid_at)
                RETURN e.name AS name, e.uuid AS uuid ORDER BY e.name, e.uuid""",
                group=self.group,
                asof=when,
                routing_='r',
            )
            direct_names = sorted(record['name'] for record in records)
            filters = SearchFilters(
                valid_at=[[
                    DateFilter(date=when, comparison_operator=ComparisonOperator.less_than_equal)
                ]],
                invalid_at=[
                    [DateFilter(date=when, comparison_operator=ComparisonOperator.greater_than)],
                    [DateFilter(comparison_operator=ComparisonOperator.is_null)],
                ],
            )
            # This is Graphiti's actual Neo4j search implementation, including
            # actual date-filter construction and real database vector comparison.
            found = await self.driver.search_ops.edge_similarity_search(
                self.driver, VECTOR, self.person, self.project, filters,
                group_ids=[self.group], limit=20, min_score=0.0,
            )
            graphiti_names = sorted(item.name for item in found)
            outcomes.append({
                'day_offset': day,
                'asof': when,
                'expected_relations': expected,
                'direct_cypher_relations': direct_names,
                'graphiti_filtered_relations': graphiti_names,
                'direct_cypher_pass': direct_names == expected,
                'graphiti_filter_pass': graphiti_names == expected,
                'query_paths_agree': direct_names == graphiti_names,
            })
        return outcomes


async def run_case(fixture):
    await fixture.initialize()
    if fixture.name == 'reactivation':
        first = await fixture.make_edge('first', 0, 10, expired=True)
        gap = await fixture.make_edge(
            'gap', 10, name='NOT_ASSIGNED',
            fact='Kiran is not assigned to the Payments Project.',
        )
        await fixture.save(first, gap)
        incoming = await fixture.make_edge('reactivated', 20)
        await fixture.resolve(
            incoming, [await fixture.load('first')], [await fixture.load('gap')],
            'reactivated', dup=[0], contra=[1],
        )
        expected_bounds = {
            'first': (moment(0), moment(10)),
            'gap': (moment(10), moment(20)),
            'reactivated': (moment(20), None),
        }

    elif fixture.name == 'finite_interval_clipping':
        later = await fixture.make_edge(
            'later', 10, name='NOT_ASSIGNED',
            fact='Kiran is not assigned to the Payments Project.',
        )
        await fixture.save(later)
        incoming = await fixture.make_edge('earlier', 0, 20)
        await fixture.resolve(
            incoming, [], [await fixture.load('later')], 'earlier', contra=[0]
        )
        expected_bounds = {
            'earlier': (moment(0), moment(10)), 'later': (moment(10), None)
        }

    elif fixture.name == 'no_context_expiry':
        incoming = await fixture.make_edge('historical', 0, 10)
        await fixture.resolve(incoming, [], [], 'historical')
        expected_bounds = {'historical': (moment(0), moment(10))}

    elif fixture.name == 'batch_period_retention':
        first = await fixture.make_edge('first', 0, 5)
        second = await fixture.make_edge('second', 10)
        first.fact_embedding = None
        second.fact_embedding = None
        clients = GraphitiClients(
            driver=fixture.driver, llm_client=fixture.llm, embedder=fixture.embedder,
            cross_encoder=fixture.reranker, tracer=NoOpTracer(),
        )
        resolved, invalidated, new = await ops.resolve_extracted_edges(
            clients, [first, second], fixture.episodes['second'], fixture.nodes, {}, {}
        )
        fixture.resolution_log.append({
            'kind': 'real_batch_resolver',
            'input_uuids': [first.uuid, second.uuid],
            'resolved_uuids': [item.uuid for item in resolved],
            'invalidated_uuids': [item.uuid for item in invalidated],
            'new_uuids': [item.uuid for item in new],
        })
        await fixture.save(*resolved, *invalidated)
        expected_bounds = {'first': (moment(0), moment(5)), 'second': (moment(10), None)}
    else:
        raise AssertionError(f'Unknown fixture: {fixture.name}')

    rows = await fixture.rows()
    by_id = {row['uuid']: row for row in rows}
    checks = []
    for ident, (start, end) in expected_bounds.items():
        row = by_id.get(f'{fixture.group}_{ident}')
        checks.append({
            'check': f'{ident}_persisted_interval',
            'expected_valid_at': start,
            'expected_invalid_at': end,
            'pass': row is not None and row['valid_at'] == start and row['invalid_at'] == end,
        })
    checks.append({
        'check': 'number_of_retained_periods',
        'expected': len(expected_bounds), 'actual': len(rows),
        'pass': len(rows) == len(expected_bounds),
    })
    if fixture.name == 'no_context_expiry':
        row = by_id.get(f'{fixture.group}_historical')
        checks.append({
            'check': 'historical_edge_has_system_expiration_metadata',
            'pass': row is not None and row['expired_at'] is not None,
        })
    queries = await fixture.query_answers()
    return {
        'case': fixture.name,
        'status': 'executed',
        'group_id': fixture.group,
        'supplied_input_edges': fixture.input_edges,
        'resolution_trace': fixture.resolution_log,
        'persisted_records': rows,
        'storage_assertions': checks,
        'temporal_queries': queries,
        'supplied_model_calls': fixture.llm.calls,
        'synthetic_embedding_calls': dict(fixture.embedder.calls),
        'cross_encoder_calls': fixture.reranker.calls,
        'pass': all(check['pass'] for check in checks) and all(
            query['direct_cypher_pass'] and query['graphiti_filter_pass'] for query in queries
        ),
    }


def source_provenance():
    names = [
        'graphiti_core/utils/maintenance/edge_operations.py',
        'graphiti_core/edges.py',
        'graphiti_core/nodes.py',
        'graphiti_core/driver/neo4j_driver.py',
        'graphiti_core/helpers.py',
        'graphiti_core/driver/record_parsers.py',
    ]
    return {
        'source_directory': str(SOURCE),
        'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'root_supplied_base_commit': PIN,
        'actual_imported_resolver': ops.__file__,
        'source_sha256': {
            name: hashlib.sha256((SOURCE / name).read_bytes()).hexdigest() for name in names
        },
        'python': platform.python_version(),
        'packages': {
            name: importlib.metadata.version(name)
            for name in ('graphiti-core', 'neo4j', 'numpy', 'pydantic')
        },
    }


async def audit(args):
    parsed = urlparse(args.uri)
    if parsed.scheme != 'bolt' or parsed.hostname not in ('127.0.0.1', 'localhost'):
        raise ValueError('Only the dedicated loopback Bolt endpoint is permitted')
    if parsed.port != 17687 or parsed.username or parsed.password:
        raise ValueError('Expected the dedicated no-auth port 17687')
    if not re.fullmatch(r'[a-z0-9_-]{1,32}', args.stage):
        raise ValueError('Stage must be a short lowercase identifier')

    run_id = uuid.uuid4().hex[:16]
    started = datetime.now(timezone.utc)
    clock_start = time.perf_counter()
    report = {
        'schema_version': '1.0',
        'stage': args.stage,
        'run_id': run_id,
        'started_at': started,
        'endpoint': args.uri,
        'data_scope': 'New unique synthetic fixture groups only; no deletion or cleanup performed',
        'provenance': source_provenance(),
        'model_api_calls': 0,
        'semantic_embedding_evaluation': False,
        'limitations': [
            'All facts, event times and model decisions are supplied synthetic fixtures.',
            'Four-dimensional constant embeddings exercise real storage/filter code, not retrieval quality.',
            'Temporal queries answer valid time after all events are ingested; knowledge-time replay is not evaluated.',
            'Original issue1841 end-only release extraction is not reproduced by this harness.',
            'Baseline and candidate use identical declared cases but different isolated group/UUID identifiers.',
            'Query counts include actual driver calls, not internal server transactions or driver network retries.',
        ],
        'cases': [],
    }
    driver = AuditedNeo4jDriver(args.uri)
    try:
        await driver.health_check()
        if driver._init_task is not None:
            await driver._init_task
        records, _, _ = await driver.execute_query(
            'CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition'
        )
        report['server_components'] = [dict(record) for record in records]
        index_rows, _, _ = await driver.execute_query(
            'SHOW INDEXES YIELD name, type, state, failureMessage '
            'RETURN name, type, state, failureMessage'
        )
        index_by_name = {row['name']: dict(row) for row in index_rows}
        required_ranges = (
            'entity_uuid', 'relation_uuid', 'entity_group_id', 'relation_group_id',
            'valid_at_edge_index', 'invalid_at_edge_index',
        )
        for index_name in required_ranges:
            row = index_by_name.get(index_name)
            if row is None or row['type'] != 'RANGE':
                raise RuntimeError(f'Required RANGE index is absent: {index_name}')
            if row['state'] != 'ONLINE':
                await driver.execute_query(
                    'CALL db.awaitIndex($index_name, 30)', index_name=index_name
                )
        index_rows, _, _ = await driver.execute_query(
            'SHOW INDEXES YIELD name, type, state, failureMessage '
            'RETURN name, type, state, failureMessage'
        )
        index_by_name = {row['name']: dict(row) for row in index_rows}
        report['required_range_indexes_online'] = all(
            index_by_name[name]['state'] == 'ONLINE' for name in required_ranges
        )
        if not report['required_range_indexes_online']:
            raise RuntimeError('Required RANGE indexes did not become ONLINE')
        # A freshly created Lucene index may still be POPULATING after RANGE
        # indexes finish. Wait for the actual index; do not replace or mock it.
        fulltext = index_by_name.get('edge_name_and_fact', {})
        if fulltext.get('state') == 'POPULATING':
            try:
                await driver.execute_query(
                    'CALL db.awaitIndex($index_name, 60)', index_name='edge_name_and_fact'
                )
            except Exception as exc:
                report['fulltext_wait_error'] = {'type': type(exc).__name__, 'message': str(exc)}
            index_rows, _, _ = await driver.execute_query(
                'SHOW INDEXES YIELD name, type, state, failureMessage '
                'RETURN name, type, state, failureMessage'
            )
            index_by_name = {row['name']: dict(row) for row in index_rows}
        report['index_states'] = [
            {
                'name': row['name'], 'type': row['type'], 'state': row['state'],
                'failure_message_excerpt': (row['failureMessage'] or '')[:2000],
                'failure_message_sha256': hashlib.sha256(
                    (row['failureMessage'] or '').encode()
                ).hexdigest(),
            }
            for row in index_by_name.values()
        ]
        fulltext_ready = (
            index_by_name.get('edge_name_and_fact', {}).get('state') == 'ONLINE'
            and index_by_name.get('edge_name_and_fact', {}).get('type') == 'FULLTEXT'
        )
        report['required_fulltext_index_online'] = fulltext_ready
        if not fulltext_ready:
            report['limitations'].append(
                'Neo4j FULLTEXT index unavailable in this environment. Real batch resolution '
                'is skipped; three storage cases and non-Lucene vector/date scans still execute. '
                'No index is dropped, replaced or mocked.'
            )
        for name in EXPECTED:
            driver.phase = name
            if name == 'batch_period_retention' and not fulltext_ready:
                report['cases'].append({
                    'case': name, 'status': 'skipped', 'pass': None,
                    'reason': 'Required edge_name_and_fact FULLTEXT index is not ONLINE',
                    'graph_writes': 0, 'supplied_model_calls': [],
                    'synthetic_embedding_calls': {},
                })
                continue
            fixture = Fixture(name, args.stage, run_id, driver)
            try:
                case = await run_case(fixture)
            except Exception as exc:
                case = {
                    'case': name, 'status': 'error', 'group_id': fixture.group, 'pass': False,
                    'error_type': type(exc).__name__, 'error': str(exc),
                    'traceback': traceback.format_exc(),
                    'supplied_input_edges': fixture.input_edges,
                    'resolution_trace': fixture.resolution_log,
                    'supplied_model_calls': fixture.llm.calls,
                    'synthetic_embedding_calls': dict(fixture.embedder.calls),
                }
            report['cases'].append(case)
    except Exception as exc:
        report['fatal_error'] = {'type': type(exc).__name__, 'message': str(exc)}
    finally:
        await driver.close()
        report['finished_at'] = datetime.now(timezone.utc)
        report['elapsed_seconds'] = time.perf_counter() - clock_start
        report['actual_database_query_calls'] = dict(driver.calls)
        report['query_text_sha256_counts'] = dict(driver.query_hashes)
        executed = [case for case in report['cases'] if case['status'] != 'skipped']
        skipped = [case for case in report['cases'] if case['status'] == 'skipped']
        report['passed_cases'] = sum(case['pass'] is True for case in executed)
        report['planned_cases'] = len(EXPECTED)
        report['executed_cases'] = len(executed)
        report['skipped_cases'] = len(skipped)
        report['executed_suite_pass'] = bool(executed) and all(
            case['pass'] for case in executed
        ) and 'fatal_error' not in report
        report['full_suite_completed'] = len(executed) == len(EXPECTED)
        report['pass'] = report['full_suite_completed'] and report['executed_suite_pass']
        report['status'] = (
            ('pass' if report['executed_suite_pass'] else 'fail')
            if report['full_suite_completed'] else
            ('partial_pass' if report['executed_suite_pass'] else 'partial_fail')
        )
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, default=json_value) + '\n', encoding='utf-8')
        print(json.dumps({
            'stage': args.stage, 'output': str(output), 'status': report['status'],
            'executed_suite_pass': report['executed_suite_pass'],
            'full_suite_completed': report['full_suite_completed'],
            'cases': [
                {key: case.get(key) for key in ('case', 'status', 'pass', 'error', 'reason')}
                for case in report['cases']
            ],
            'actual_database_query_calls': dict(driver.calls),
            'fatal_error': report.get('fatal_error'),
        }, indent=2))
    success = report['pass'] if args.require_fulltext else report['executed_suite_pass']
    return 0 if success else 1


async def audit_roundtrip(args):
    """No resolver invocation: isolate unchanged save/read/save representation."""
    parsed = urlparse(args.uri)
    if (
        parsed.scheme != 'bolt' or parsed.hostname not in ('127.0.0.1', 'localhost')
        or parsed.port != 17687 or parsed.username or parsed.password
    ):
        raise ValueError('Only the dedicated no-auth loopback Bolt port 17687 is permitted')
    if not re.fullmatch(r'[a-z0-9_-]{1,32}', args.stage):
        raise ValueError('Stage must be a short lowercase identifier')

    def describe_dates(item):
        result = {}
        for key in ('valid_at', 'invalid_at', 'created_at', 'expired_at', 'reference_time'):
            value = getattr(item, key)
            result[key] = None if value is None else {
                'iso': value.isoformat(),
                'python_type': f'{type(value).__module__}.{type(value).__name__}',
                'tzinfo_type': f'{type(value.tzinfo).__module__}.{type(value.tzinfo).__name__}',
                'tzinfo_repr': repr(value.tzinfo),
                'tzname': value.tzname(),
                'epoch_seconds': value.timestamp(),
            }
        return result

    async def database_values(driver, group):
        records, _, _ = await driver.execute_query(
            """MATCH ()-[e:RELATES_TO]->() WHERE e.group_id = $group
            RETURN e.uuid AS uuid,
              toString(e.valid_at) AS valid_text, toString(e.invalid_at) AS invalid_text,
              toString($start) AS start_parameter_text, toString($end) AS end_parameter_text,
              e.valid_at.epochSeconds AS valid_epoch, e.invalid_at.epochSeconds AS invalid_epoch,
              $start.epochSeconds AS start_parameter_epoch, $end.epochSeconds AS end_parameter_epoch,
              e.valid_at.nanosecond AS valid_nanosecond, e.invalid_at.nanosecond AS invalid_nanosecond,
              e.valid_at = $start AS start_equal, e.invalid_at = $end AS end_equal,
              (e.valid_at <= $start AND $start < e.invalid_at) AS active_at_start,
              (e.valid_at <= $end AND $end < e.invalid_at) AS active_at_end""",
            group=group, start=moment(0), end=moment(10), routing_='r',
        )
        return [dict(record) for record in records]

    run_id = uuid.uuid4().hex[:16]
    report = {
        'schema_version': '1.0', 'stage': args.stage, 'run_id': run_id,
        'kind': 'unchanged_edge_save_read_save',
        'started_at': datetime.now(timezone.utc),
        'provenance': source_provenance(),
        'expected': {'active_at_start': True, 'active_at_end': False},
        'resolver_calls': 0, 'model_api_calls': 0,
        'scope': 'One unique synthetic group; real saves, read parser and direct Cypher only',
        'fulltext_indexes_required': False,
    }
    driver = AuditedNeo4jDriver(args.uri)
    try:
        await driver.health_check()
        if driver._init_task is not None:
            await driver._init_task
        driver.phase = 'unchanged_roundtrip'
        fixture = Fixture('unchanged_resave', args.stage, run_id, driver)
        await fixture.initialize()
        report['group_id'] = fixture.group
        item = await fixture.make_edge('unchanged', 0, 10, expired=True)
        report['before_initial_save_python_values'] = describe_dates(item)
        await fixture.save(item)
        report['after_initial_save_database_values'] = await database_values(driver, fixture.group)
        loaded = await fixture.load('unchanged')
        report['after_read_python_values'] = describe_dates(loaded)
        await fixture.save(loaded)
        report['after_unchanged_resave_database_values'] = await database_values(driver, fixture.group)
        reread = await fixture.load('unchanged')
        report['after_second_read_python_values'] = describe_dates(reread)
        before = report['after_initial_save_database_values']
        after = report['after_unchanged_resave_database_values']
        report['unchanged_temporal_answers'] = len(before) == len(after) == 1 and all(
            row['active_at_start'] is True and row['active_at_end'] is False
            and row['start_equal'] is True and row['end_equal'] is True
            for row in before + after
        )
        report['unchanged_native_values'] = len(before) == len(after) == 1 and all(
            before[0][key] == after[0][key]
            for key in ('uuid', 'valid_text', 'invalid_text', 'valid_epoch', 'invalid_epoch',
                        'valid_nanosecond', 'invalid_nanosecond')
        )
    except Exception as exc:
        report['fatal_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        report['unchanged_temporal_answers'] = False
    finally:
        await driver.close()
        report['finished_at'] = datetime.now(timezone.utc)
        report['actual_database_query_calls'] = dict(driver.calls)
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, default=json_value) + '\n', encoding='utf-8')
        print(json.dumps({
            'output': str(output), 'stage': args.stage,
            'unchanged_temporal_answers': report['unchanged_temporal_answers'],
            'before': report.get('after_initial_save_database_values'),
            'after': report.get('after_unchanged_resave_database_values'),
            'fatal_error': report.get('fatal_error'),
        }, indent=2))
    return 0 if report['unchanged_temporal_answers'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', required=True)
    parser.add_argument('--uri', default='bolt://127.0.0.1:17687')
    parser.add_argument('--output', required=True)
    parser.add_argument('--roundtrip-only', action='store_true')
    parser.add_argument('--require-fulltext', action='store_true',
                        help='Fail the process if any planned database case is skipped')
    args = parser.parse_args()
    return asyncio.run(audit_roundtrip(args) if args.roundtrip_only else audit(args))


if __name__ == '__main__':
    raise SystemExit(main())
