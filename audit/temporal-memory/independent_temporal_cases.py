"""Finite, no-service tests of real Graphiti temporal-resolution helpers.

Run with the campaign Python interpreter. The sibling graphiti directory is the
real source checkout; all third-party imports must be genuinely installed.
Model decisions below are supplied fixtures, NOT measured LLM performance.
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ['GRAPHITI_TELEMETRY_ENABLED'] = 'false'
for backend in ('NEO4J', 'FALKORDB', 'KUZU', 'NEPTUNE'):
    os.environ[f'DISABLE_{backend}'] = '1'

SOURCE = Path(os.environ.get(
    'GRAPHITI_TEST_SOURCE', str(Path(__file__).resolve().parent / 'graphiti')
)).resolve()
sys.path.insert(0, str(SOURCE))

from graphiti_core.edges import EntityEdge
from graphiti_core.llm_client import LLMClient
from graphiti_core.nodes import EntityNode, EpisodicNode
from graphiti_core.search.search_config import SearchResults
from graphiti_core.utils.maintenance import edge_operations as ops

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
INGESTED = datetime(2026, 3, 1, tzinfo=timezone.utc)
FACT = 'Kiran is assigned to the Payments Project.'


def at(day: int) -> datetime:
    return BASE + timedelta(days=day)


def edge(
    ident: str,
    start: int | None,
    end: int | None = None,
    *,
    fact: str = FACT,
    target: str = 'payments',
    name: str = 'ASSIGNED',
) -> EntityEdge:
    return EntityEdge(
        uuid=ident,
        source_node_uuid='kiran',
        target_node_uuid=target,
        group_id='independent-temporal-fixture',
        name=name,
        fact=fact,
        episodes=[f'evidence-{ident}'],
        created_at=INGESTED - timedelta(days=1),
        valid_at=None if start is None else at(start),
        invalid_at=None if end is None else at(end),
        expired_at=None if end is None else INGESTED - timedelta(hours=1),
    )


def episode() -> EpisodicNode:
    return EpisodicNode(
        uuid='current-episode',
        name='Supplied temporal fixture',
        source='message',
        group_id='independent-temporal-fixture',
        source_description='Synthetic controlled evidence; no model evaluation',
        content='See the explicitly supplied fact, timestamps and contradiction decisions.',
        valid_at=INGESTED,
    )


def scripted_client(
    duplicates: list[int] | None = None,
    contradictions: list[int] | None = None,
) -> Mock:
    """Only actual response-model seams are mocked; unexpected calls fail."""
    async def response(*args, **kwargs):
        name = kwargs.get('prompt_name')
        if name == 'dedupe_edges.resolve_edge':
            return {
                'duplicate_facts': [] if duplicates is None else duplicates,
                'contradicted_facts': [] if contradictions is None else contradictions,
            }
        if name == 'extract_edges.extract_timestamps':
            # Unavailable evidence must remain unavailable, not become wall time.
            return {'valid_at': None, 'invalid_at': None}
        raise AssertionError(f'Unexpected model call: {name}')

    client = Mock(spec=LLMClient)
    client.generate_response = AsyncMock(side_effect=response)
    return client


async def resolve(incoming, related, broader=(), *, duplicates=None, contradictions=None):
    client = scripted_client(duplicates, contradictions)
    with patch.object(ops, 'utc_now', return_value=INGESTED):
        result = await ops.resolve_extracted_edge(
            client, incoming, list(related), list(broader), episode(), {}
        )
    return result, client


class TemporalDuplicateGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_separated_or_touching_intervals_survive_wrong_duplicate_decision(self):
        # Existing first, incoming second. Four cases cover both event-time orders.
        cases = (
            ('gap_forward', (0, 5), (10, None)),
            ('gap_reverse', (10, None), (0, 5)),
            ('touch_forward', (0, 10), (10, None)),
            ('touch_reverse', (10, None), (0, 10)),
        )
        for label, old_window, new_window in cases:
            with self.subTest(label=label):
                old = edge('old', *old_window)
                incoming = edge('new', *new_window)
                old_before = old.model_dump()
                expected_window = (incoming.valid_at, incoming.invalid_at)
                (resolved, invalidated, duplicates), _ = await resolve(
                    incoming, [old], duplicates=[0]
                )
                self.assertEqual(
                    resolved.uuid, incoming.uuid,
                    'Known separate half-open intervals must not share a duplicate identity',
                )
                self.assertEqual((resolved.valid_at, resolved.invalid_at), expected_window)
                self.assertEqual(old.model_dump(), old_before)
                self.assertEqual(invalidated, [])
                self.assertEqual(duplicates, [])

    async def test_model_duplicate_cannot_merge_nonverbatim_disjoint_intervals(self):
        # Avoid the exact-text shortcut: this specifically reaches the supplied
        # model's duplicate decision. These are separate bounded event periods.
        old = edge('old', 0, 5)
        incoming = edge('new', 10, fact='Kiran has an assignment on the Payments Project.')
        (resolved, invalidated, duplicates), client = await resolve(
            incoming, [old], duplicates=[0]
        )
        self.assertEqual(resolved.uuid, 'new')
        self.assertEqual((resolved.valid_at, resolved.invalid_at), (at(10), None))
        self.assertEqual(old.invalid_at, at(5))
        self.assertEqual(invalidated, [])
        self.assertEqual(duplicates, [])
        self.assertTrue(any(
            call.kwargs.get('prompt_name') == 'dedupe_edges.resolve_edge'
            for call in client.generate_response.await_args_list
        ))

    async def test_closed_history_and_unknown_new_time_do_not_invent_now(self):
        old = edge('old', 0, 5)
        incoming = edge('unknown-time', None)
        (resolved, invalidated, duplicates), _ = await resolve(
            incoming, [old], duplicates=[0]
        )
        self.assertEqual(resolved.uuid, 'unknown-time')
        self.assertIsNone(resolved.valid_at)
        self.assertIsNone(resolved.invalid_at)
        self.assertEqual(old.invalid_at, at(5))
        self.assertEqual(invalidated, [])
        self.assertEqual(duplicates, [])

    async def test_identical_intervals_keep_existing_duplicate_behavior(self):
        for window in ((0, None), (0, 10)):
            with self.subTest(window=window):
                old = edge('old', *window)
                incoming = edge('same-period', *window)
                expected_window = (old.valid_at, old.invalid_at)
                (resolved, invalidated, _), _ = await resolve(
                    incoming, [old], duplicates=[0]
                )
                self.assertEqual(resolved.uuid, old.uuid)
                self.assertEqual((resolved.valid_at, resolved.invalid_at), expected_window)
                self.assertIn('current-episode', resolved.episodes)
                self.assertEqual(invalidated, [])

    async def test_partial_overlap_keeps_existing_duplicate_behavior(self):
        old = edge('old', 0, 20)
        incoming = edge('overlap', 10, 30)
        (resolved, invalidated, _), _ = await resolve(
            incoming, [old], duplicates=[0]
        )
        # Compatibility assertion only; this does not endorse an interval-union
        # or coverage policy beyond Graphiti's pre-existing duplicate behavior.
        self.assertEqual(resolved.uuid, old.uuid)
        self.assertEqual(invalidated, [])

    async def test_assignment_release_reassignment_preserves_three_periods(self):
        first = edge('first-assignment', 0, 10)
        released = edge(
            'released', 10, fact='Kiran is not assigned to the Payments Project.',
            name='NOT_ASSIGNED',
        )
        incoming = edge('reassignment', 20)
        (resolved, invalidated, duplicates), _ = await resolve(
            incoming, [first], [released], duplicates=[0], contradictions=[1]
        )
        self.assertEqual(resolved.uuid, 'reassignment')
        self.assertEqual((resolved.valid_at, resolved.invalid_at), (at(20), None))
        self.assertEqual((first.valid_at, first.invalid_at), (at(0), at(10)))
        self.assertEqual((released.valid_at, released.invalid_at), (at(10), at(20)))
        self.assertEqual([item.uuid for item in invalidated], ['released'])
        self.assertEqual(duplicates, [])


class SuppliedDecisionControls(unittest.IsolatedAsyncioTestCase):
    async def test_declared_release_time_closes_only_selected_assignment(self):
        prior = edge('assignment', 0)
        incoming = edge('release', 10, fact='Kiran was released.', name='RELEASED')
        old_evidence = prior.episodes.copy()
        (resolved, invalidated, duplicates), _ = await resolve(
            incoming, [prior], contradictions=[0]
        )
        self.assertEqual(resolved.uuid, 'release')
        self.assertEqual([item.uuid for item in invalidated], ['assignment'])
        self.assertEqual(prior.invalid_at, at(10))
        self.assertEqual(prior.expired_at, INGESTED)
        self.assertEqual(prior.episodes, old_evidence)
        self.assertEqual(duplicates, [])

    async def test_negated_release_with_no_supplied_contradiction_retains_assignment(self):
        prior = edge('assignment', 0)
        incoming = edge('negated', 10, fact='Kiran was not released from Payments.')
        (resolved, invalidated, duplicates), _ = await resolve(incoming, [prior])
        self.assertEqual(resolved.uuid, 'negated')
        self.assertIsNone(prior.invalid_at)
        self.assertIsNone(prior.expired_at)
        self.assertEqual(invalidated, [])
        self.assertEqual(duplicates, [])

    async def test_concurrent_project_without_supplied_contradiction_is_preserved(self):
        prior = edge('payments', 0)
        incoming = edge(
            'copilot', 10, fact='Kiran also works on AI Copilot while retaining Payments.',
            target='copilot',
        )
        (_, invalidated, _), _ = await resolve(incoming, [], [prior])
        self.assertIsNone(prior.invalid_at)
        self.assertEqual(invalidated, [])

    async def test_older_event_arriving_late_is_bounded_by_known_later_event(self):
        later = edge('release', 10, fact='Kiran was released.', name='RELEASED')
        older = edge('late-assignment', 0)
        (resolved, invalidated, _), _ = await resolve(
            older, [], [later], contradictions=[0]
        )
        self.assertEqual((resolved.valid_at, resolved.invalid_at), (at(0), at(10)))
        self.assertEqual(resolved.expired_at, INGESTED)
        self.assertIsNone(later.invalid_at)
        self.assertEqual(invalidated, [])

    async def test_reported_end_only_release_does_not_establish_original_issue_fixed(self):
        # Characterize the issue's reported timestamp shape, without inventing
        # a start time. This passing test must NOT be marketed as desired behavior.
        prior = edge('assignment', 0)
        incoming = edge('release', None, 10, fact='Kiran was released.', name='RELEASED')
        (_, invalidated, _), _ = await resolve(incoming, [prior], contradictions=[0])
        self.assertIsNone(prior.invalid_at)
        self.assertEqual(invalidated, [])


class PureIntervalControls(unittest.TestCase):
    def test_nonoverlap_including_touching_does_not_expire_history(self):
        for old_window, new_window in (
            ((0, 5), (10, None)), ((0, 10), (10, None)),
            ((10, None), (0, 5)), ((10, None), (0, 10)),
        ):
            with self.subTest(old=old_window, new=new_window):
                prior = edge('prior', *old_window)
                incoming = edge('incoming', *new_window)
                before = prior.model_dump()
                self.assertEqual(ops.resolve_edge_contradictions(incoming, [prior]), [])
                self.assertEqual(prior.model_dump(), before)

    def test_timezone_equivalent_touching_boundary_has_no_overlap(self):
        prior = edge('prior', 0, 10)
        incoming = edge('incoming', 10)
        incoming.valid_at = at(10).astimezone(timezone(timedelta(hours=7)))
        self.assertEqual(ops.resolve_edge_contradictions(incoming, [prior]), [])
        self.assertEqual(prior.invalid_at, at(10))

    def test_unknown_start_is_not_replaced_with_ingestion_time(self):
        prior = edge('prior', 0)
        unknown = edge('unknown', None)
        self.assertEqual(ops.resolve_edge_contradictions(unknown, [prior]), [])
        self.assertIsNone(prior.invalid_at)
        self.assertIsNone(unknown.valid_at)


class BatchTemporalControls(unittest.IsolatedAsyncioTestCase):
    async def run_batch(self, supplied):
        client = scripted_client()
        clients = SimpleNamespace(
            driver=Mock(), llm_client=client, embedder=Mock(), cross_encoder=Mock()
        )
        nodes = [
            EntityNode(uuid='kiran', name='Kiran', group_id='independent-temporal-fixture'),
            EntityNode(uuid='payments', name='Payments', group_id='independent-temporal-fixture'),
        ]
        # Stub I/O boundaries only. Real dedup, resolution, model parsing and
        # EntityEdge objects are used; no package or algorithm is reimplemented.
        with (
            patch.object(EntityEdge, 'get_between_nodes', AsyncMock(return_value=[])),
            patch.object(ops, 'search', AsyncMock(return_value=SearchResults())),
            patch.object(ops, 'create_entity_edge_embeddings', AsyncMock(return_value=None)),
        ):
            result = await ops.resolve_extracted_edges(
                clients, supplied, episode(), nodes, {}, {}
            )
        client.generate_response.assert_not_awaited()
        return result

    async def test_batch_retains_distinct_time_windows_in_both_input_orders(self):
        for end in (5, 10):
            for reverse in (False, True):
                with self.subTest(end=end, reverse=reverse):
                    first = edge('first', 0, end)
                    second = edge('second', 10)
                    inputs = [second, first] if reverse else [first, second]
                    resolved, invalidated, new = await self.run_batch(inputs)
                    self.assertEqual({item.uuid for item in resolved}, {'first', 'second'})
                    self.assertEqual(
                        {item.uuid: (item.valid_at, item.invalid_at) for item in resolved},
                        {'first': (at(0), at(end)), 'second': (at(10), None)},
                    )
                    self.assertEqual({item.uuid for item in new}, {'first', 'second'})
                    self.assertEqual(invalidated, [])

    async def test_batch_still_collapses_identical_fact_and_interval(self):
        first = edge('first', 0, 5)
        second = edge('second', 0, 5, fact='  KIRAN is assigned to the Payments Project. ')
        resolved, invalidated, new = await self.run_batch([first, second])
        self.assertEqual(len(resolved), 1)
        self.assertEqual([item.uuid for item in new], ['first'])
        self.assertEqual(invalidated, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
