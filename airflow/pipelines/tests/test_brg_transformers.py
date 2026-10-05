import unittest
from dataclasses import asdict
from unittest.mock import AsyncMock
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.query import QueryResult

from support import BRG_PIPELINES, make_source, snapshot


class BrgTransformerTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_batches(self):
        for pipeline in BRG_PIPELINES:
            with self.subTest(pipeline=pipeline.__name__):
                client = AsyncMock(spec=AsyncClient)
                self.assertEqual(await pipeline.transformer.transform([], client, load_id=UUID(int=10)), [])
                client.query.assert_not_awaited()

    async def test_mapping_and_duplicate_ids(self):
        for pipeline in BRG_PIPELINES:
            with self.subTest(pipeline=pipeline.__name__):
                source = pipeline.extractor.model
                batch = [make_source(source, 0), make_source(source, 1), make_source(source, 0)]
                batch.append(batch[0])
                before = snapshot(batch)
                sk_names = [name for name in pipeline.loader.model.column_names() if name.endswith("_sk")]
                maps = []
                responses = []
                for sk in sk_names:
                    key = sk.removesuffix("_sk") + "_id"
                    ids = list(dict.fromkeys(getattr(row, key) for row in batch))
                    mapping = dict(zip(ids, (0, 42), strict=True))
                    maps.append(mapping)
                    responses.append(QueryResult(result_set=list(mapping.items())))
                client = AsyncMock(spec=AsyncClient)
                client.query.side_effect = responses
                load_id = UUID(int=10)
                converted = await pipeline.transformer.transform(batch, client, load_id=load_id)
                self.assertEqual(len(converted), len(batch))
                for original, target in zip(batch, converted, strict=True):
                    expected = {"load_id": load_id}
                    for sk, mapping in zip(sk_names, maps, strict=True):
                        expected[sk] = mapping[getattr(original, sk.removesuffix("_sk") + "_id")]
                    for name in pipeline.loader.model.column_names():
                        if name not in expected:
                            expected[name] = getattr(original, name)
                    self.assertEqual(asdict(target), expected)
                self.assertEqual(snapshot(batch), before)
                self.assertEqual(client.query.await_count, 2)
                for call, mapping in zip(client.query.await_args_list, maps, strict=True):
                    self.assertEqual(call.kwargs["parameters"], {"entity_ids": list(mapping)})
                client.insert.assert_not_awaited()
                client.close.assert_not_awaited()

    async def test_missing_entities_and_query_errors(self):
        for pipeline in BRG_PIPELINES:
            for side in (0, 1):
                for failure in ("missing", "query"):
                    with self.subTest(pipeline=pipeline.__name__, side=side, failure=failure):
                        batch = [make_source(pipeline.extractor.model)]
                        before = snapshot(batch)
                        sk_names = [name for name in pipeline.loader.model.column_names() if name.endswith("_sk")]
                        keys = [getattr(batch[0], name.removesuffix("_sk") + "_id") for name in sk_names]
                        responses = [QueryResult(result_set=[(key, 0)]) for key in keys]
                        error = RuntimeError("query failed")
                        responses[side] = QueryResult(result_set=[]) if failure == "missing" else error
                        client = AsyncMock(spec=AsyncClient)
                        client.query.side_effect = responses
                        with self.assertRaises(LookupError if failure == "missing" else RuntimeError) as caught:
                            await pipeline.transformer.transform(batch, client, load_id=UUID(int=10))
                        if failure == "missing":
                            self.assertIn(str(keys[side]), str(caught.exception))
                        else:
                            self.assertIs(caught.exception, error)
                        self.assertEqual(client.query.await_count, side + 1)
                        self.assertEqual(snapshot(batch), before)
                        client.insert.assert_not_awaited()
                        client.close.assert_not_awaited()
