import importlib
import unittest
from dataclasses import asdict
from unittest.mock import AsyncMock, patch
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.binding import bind_query
from clickhouse_connect.driver.query import QueryResult

from support import DIM_PIPELINES, make_source, snapshot


class DimTransformerTests(unittest.IsolatedAsyncioTestCase):
    def mapper(self, pipeline):
        name = pipeline.loader.model.table_name.removeprefix("music_dwh.dim_")
        module = importlib.import_module(f"src.mappers.{name}_mapper")
        mapper_name = pipeline.transformer.__name__.removeprefix("Dim").removesuffix("Transformer") + "Mapper"
        return getattr(module, mapper_name), name

    async def test_empty_batches(self):
        for pipeline in DIM_PIPELINES:
            with self.subTest(pipeline=pipeline.__name__):
                client = AsyncMock(spec=AsyncClient)
                mapper, _ = self.mapper(pipeline)
                with patch.object(mapper, "to_dwh", wraps=mapper.to_dwh) as call:
                    self.assertEqual(await pipeline.transformer.transform([], client, load_id=UUID(int=10)), [])
                    call.assert_not_called()
                client.query.assert_not_awaited()

    async def test_mapping_and_lookup(self):
        for pipeline in DIM_PIPELINES:
            mapper, name = self.mapper(pipeline)
            key, sk = name + "_id", name + "_sk"
            for case in ("new", "mixed", "existing", "duplicates", "same_object"):
                with self.subTest(pipeline=pipeline.__name__, case=case):
                    batch = [make_source(pipeline.extractor.model, index) for index in range(3)]
                    if case == "duplicates":
                        batch.append(make_source(pipeline.extractor.model, 0))
                    if case == "same_object":
                        batch.append(batch[0])
                    before = snapshot(batch)
                    identities = [id(row) for row in batch]
                    ids = list(dict.fromkeys(row.id for row in batch))
                    rows = [] if case == "new" else [(ids[0], 0), (ids[1], 42)]
                    if case == "existing":
                        rows.append((ids[2], 99))
                    client = AsyncMock(spec=AsyncClient)
                    client.query.return_value = QueryResult(result_set=rows, column_names=(key, sk))
                    load_id = UUID(int=10)
                    transformed = await pipeline.transformer.transform(batch, client, load_id=load_id)
                    self.assertIsNot(transformed, batch)
                    self.assertEqual(len(transformed), len(batch))
                    for original, target in zip(batch, transformed, strict=True):
                        expected = mapper.to_dwh(original, load_id, **{sk: dict(rows).get(original.id)})
                        self.assertEqual(asdict(target), asdict(expected))
                        self.assertIsInstance(target, pipeline.loader.model)
                    self.assertEqual(snapshot(batch), before)
                    self.assertEqual([id(row) for row in batch], identities)
                    client.query.assert_awaited_once()
                    kwargs = client.query.await_args.kwargs
                    version = {"app_release": "source_created_at", "device_os": "valid_from"}.get(name, "updated_at")
                    sql = f"SELECT {key}, argMax({sk}, ({version}, {sk})) AS {sk} FROM {pipeline.loader.model.table_to_insert()} WHERE {key} IN {{{name}_ids:Array(UUID)}} GROUP BY {key}"
                    self.assertEqual(" ".join(kwargs["query"].split()), sql)
                    self.assertEqual(kwargs["parameters"], {name + "_ids": ids})
                    _, bound = bind_query(kwargs["query"], kwargs["parameters"])
                    self.assertIn("param_" + name + "_ids", bound)
                    client.insert.assert_not_awaited()
                    client.close.assert_not_awaited()

    async def test_query_and_mapper_errors_do_not_mutate_source(self):
        for pipeline in DIM_PIPELINES:
            mapper, _ = self.mapper(pipeline)
            for stage in ("query", "mapper"):
                with self.subTest(pipeline=pipeline.__name__, stage=stage):
                    batch = [make_source(pipeline.extractor.model)]
                    before = snapshot(batch)
                    client = AsyncMock(spec=AsyncClient)
                    error = RuntimeError(stage)
                    client.query.return_value = QueryResult(result_set=[])
                    if stage == "query":
                        client.query.side_effect = error
                    with patch.object(mapper, "to_dwh", side_effect=error) as call:
                        with self.assertRaises(RuntimeError) as caught:
                            await pipeline.transformer.transform(batch, client, load_id=UUID(int=10))
                        if stage == "query":
                            call.assert_not_called()
                    self.assertIs(caught.exception, error)
                    self.assertEqual(snapshot(batch), before)
