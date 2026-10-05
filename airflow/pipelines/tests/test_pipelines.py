import asyncio
import subprocess
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from src.models.dwh import EtlLoad
from src.pipelines import DimDevicePipeline

from support import END, PIPELINES, START, Harness, dwh_settings, make_source, oltp_settings, snapshot


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def run_pipeline(self, pipeline, harness, **overrides):
        arguments = {
            "timestamp_start": START,
            "timestamp_end": END,
            "dag_id": "test_dag",
            "run_id": "test_run",
            "task_id": pipeline.__name__,
            "oltp_settings": oltp_settings(),
            "dwh_settings": dwh_settings(),
        }
        arguments.update(overrides)
        with (
            patch("src.pipelines._pipeline.DwhConnector", side_effect=harness.dwh_context) as dwh,
            patch("src.pipelines._pipeline.OltpConnector", side_effect=harness.oltp_context) as oltp,
        ):
            try:
                return await pipeline.run(**arguments)
            finally:
                self.assertLessEqual(dwh.call_count, 1)
                self.assertLessEqual(oltp.call_count, 1)

    async def test_all_twelve_pipelines(self):
        self.assertEqual(len(PIPELINES), 12)
        for pipeline in PIPELINES:
            with self.subTest(pipeline=pipeline.__name__):
                source = pipeline.extractor.model
                batches = [[make_source(source, 0), make_source(source, 1)], [make_source(source, 2)]]
                harness = Harness(batches)
                before = [snapshot(batch) for batch in batches]
                extractor_init = pipeline.extractor.__init__
                loader_init = pipeline.loader.__init__
                with (
                    patch.object(pipeline.extractor, "__init__", autospec=True, side_effect=extractor_init) as extractor,
                    patch.object(pipeline.loader, "__init__", autospec=True, side_effect=loader_init) as loader,
                    patch.object(pipeline.transformer, "transform", wraps=pipeline.transformer.transform) as transform,
                ):
                    audit = await self.run_pipeline(pipeline, harness)
                    extractor.assert_called_once()
                    loader.assert_called_once()
                    self.assertIs(extractor.call_args.args[1], harness.session)
                    self.assertIs(loader.call_args.args[1], harness.client)
                    self.assertEqual(transform.await_count, 2)
                    for call, batch in zip(transform.await_args_list, batches, strict=True):
                        self.assertEqual(call.args[0], batch)
                        for actual, original in zip(call.args[0], batch, strict=True):
                            self.assertIs(actual, original)
                        self.assertIs(call.args[1], harness.client)
                        self.assertEqual(call.kwargs, {"load_id": audit.load_id})
                self.assertIsInstance(audit, EtlLoad)
                self.assertEqual((audit.rows_read, audit.rows_written, audit.rows_rejected), (3, 3, 0))
                self.assertEqual(audit.state, "success")
                self.assertIsNone(audit.error_msg)
                self.assertEqual(audit.source_system, "music_oltp")
                self.assertEqual(audit.target_table, pipeline.loader.model.table_to_insert())
                self.assertEqual((audit.dag_id, audit.run_id, audit.task_id), ("test_dag", "test_run", pipeline.__name__))
                self.assertEqual((audit.interval_start, audit.interval_end), (START, END))
                self.assertEqual(audit.started_at.utcoffset().total_seconds(), 0)
                self.assertGreaterEqual(audit.finished_at, audit.started_at)
                self.assertEqual([row["state"] for row in harness.audits], ["running", "success"])
                self.assertEqual(harness.audits[0]["rows_read"], 0)
                self.assertIsNone(harness.audits[0]["finished_at"])
                self.assertEqual({row["load_id"] for row in harness.audits}, {audit.load_id})
                written = [row for table, rows in harness.inserts if table == audit.target_table for row in rows]
                self.assertEqual(len(written), 3)
                self.assertEqual({row["load_id"] for row in written}, {audit.load_id})
                self.assertEqual([snapshot(batch) for batch in batches], before)
                self.assertEqual(harness.partition_size, 2)
                harness.result.close.assert_awaited_once()
                harness.session.close.assert_awaited_once()
                harness.client.close.assert_awaited_once()
                self.assertLess(harness.events.index("result_close"), harness.events.index("session_close"))
                self.assertLess(harness.events.index("session_close"), harness.events.index("oltp_close"))
                self.assertLess(harness.events.index("oltp_close"), harness.events.index("dwh_close"))
                self.assertLess(harness.events.index("insert:music_dwh.etl_loads"), harness.events.index("extract"))
                self.assertLess(harness.events.index("insert:" + audit.target_table), harness.events.index("batch:1"))

    async def test_no_data_and_equal_interval(self):
        for pipeline in PIPELINES:
            for equal in (False, True):
                with self.subTest(pipeline=pipeline.__name__, equal=equal):
                    harness = Harness([])
                    audit = await self.run_pipeline(pipeline, harness, timestamp_end=START if equal else END)
                    self.assertEqual((audit.rows_read, audit.rows_written), (0, 0))
                    self.assertEqual([row["state"] for row in harness.audits], ["running", "success"])
                    self.assertTrue(all(table == "music_dwh.etl_loads" for table, _ in harness.inserts))
                    harness.client.query.assert_not_awaited()
                    if equal:
                        harness.session.stream_scalars.assert_not_awaited()

    async def test_dim_pipeline_omits_generated_sk_only_for_new_rows(self):
        for pipeline in PIPELINES:
            if not pipeline.__name__.startswith("Dim"):
                continue
            with self.subTest(pipeline=pipeline.__name__):
                batch = [make_source(pipeline.extractor.model, index) for index in range(2)]
                harness = Harness([batch])
                original_query = harness.query

                async def query(*, query, parameters, current=original_query):
                    result = await current(query=query, parameters=parameters)
                    result.result_rows.pop()
                    return result

                harness.client.query.side_effect = query
                audit = await self.run_pipeline(pipeline, harness)
                data = [rows for table, rows in harness.inserts if table == audit.target_table]
                sk = pipeline.loader.generated_columns[0]
                self.assertEqual(len(data), 2)
                self.assertEqual(data[0][0][sk], 0)
                self.assertNotIn(sk, data[1][0])
                self.assertEqual((audit.rows_read, audit.rows_written), (2, 2))

    async def test_missing_bridge_dimensions_fail_pipeline(self):
        from clickhouse_connect.driver.query import QueryResult

        for pipeline in PIPELINES:
            if not pipeline.__name__.startswith("Brg"):
                continue
            for side in (0, 1):
                with self.subTest(pipeline=pipeline.__name__, side=side):
                    batch = [make_source(pipeline.extractor.model)]
                    harness = Harness([batch])
                    count = 0

                    async def query(*, query, parameters):
                        nonlocal count
                        count += 1
                        if count == side + 1:
                            return QueryResult(result_set=[])
                        return await harness.query(query=query, parameters=parameters)

                    harness.client.query.side_effect = query
                    with self.assertRaises(LookupError):
                        await self.run_pipeline(pipeline, harness)
                    self.assertEqual([row["state"] for row in harness.audits], ["running", "failed"])
                    self.assertEqual((harness.audits[-1]["rows_read"], harness.audits[-1]["rows_written"]), (1, 0))
                    self.assertTrue(all(table == "music_dwh.etl_loads" for table, _ in harness.inserts))
                    harness.result.close.assert_awaited_once()
                    harness.session.close.assert_awaited_once()
                    harness.client.close.assert_awaited_once()

    async def test_invalid_interval_precedes_settings_and_connections(self):
        for start, end in ((START.replace(tzinfo=None), END), (START, END.replace(tzinfo=None)), (END, START)):
            with (
                self.subTest(start=start, end=end),
                patch("src.pipelines._pipeline.OltpSettings") as settings,
                patch("src.pipelines._pipeline.DwhConnector") as dwh,
                patch("src.pipelines._pipeline.OltpConnector") as oltp,
            ):
                with self.assertRaises(ValueError):
                    await DimDevicePipeline.run(timestamp_start=start, timestamp_end=end, dag_id="dag", run_id="run", task_id="task")
                settings.assert_not_called()
                dwh.assert_not_called()
                oltp.assert_not_called()

    async def test_failures_stop_loading_and_close_resources(self):
        pipeline = DimDevicePipeline
        for stage in ("extractor", "partitions", "mapper", "transformer", "loader", "oltp"):
            with self.subTest(stage=stage), ExitStack() as stack:
                batches = [[make_source(pipeline.extractor.model, 0)], [make_source(pipeline.extractor.model, 1)]]
                harness = Harness(batches)
                error = RuntimeError(stage)
                if stage == "extractor":
                    harness.stream_error = error
                elif stage == "partitions":
                    harness.partition_error = error
                elif stage == "mapper":
                    stack.enter_context(patch("src.transformers.dim_device_transformer.DeviceMapper.to_dwh", side_effect=error))
                elif stage == "transformer":
                    harness.query_error = error
                elif stage == "loader":
                    harness.data_insert_error = error
                elif stage == "oltp":
                    harness.oltp_error = error
                with self.assertRaises(RuntimeError) as caught:
                    await self.run_pipeline(pipeline, harness)
                self.assertIs(caught.exception, error)
                self.assertEqual([row["state"] for row in harness.audits], ["running", "failed"])
                failed = harness.audits[-1]
                self.assertIn(stage, failed["error_msg"])
                self.assertIsNotNone(failed["finished_at"])
                expected_counts = (0, 0) if stage in ("extractor", "oltp") else (1, 1) if stage == "partitions" else (1, 0)
                self.assertEqual((failed["rows_read"], failed["rows_written"]), expected_counts)
                harness.client.close.assert_awaited_once()
                if stage != "oltp":
                    harness.session.close.assert_awaited_once()
                if stage not in ("extractor", "oltp"):
                    harness.result.close.assert_awaited_once()
                    self.assertLess(harness.events.index("result_close"), harness.events.index("session_close"))
                if stage != "partitions":
                    self.assertNotIn("batch:1", harness.events)

    async def test_loader_failure_preserves_previous_batch_count(self):
        pipeline = DimDevicePipeline
        harness = Harness([[make_source(pipeline.extractor.model, index)] for index in range(3)])
        error = RuntimeError("second batch failed")
        original_insert = harness.insert
        count = 0

        async def insert(**kwargs):
            nonlocal count
            if kwargs["table"] != "music_dwh.etl_loads":
                count += 1
                if count == 2:
                    harness.data_insert_error = error
            return await original_insert(**kwargs)

        harness.client.insert.side_effect = insert
        with self.assertRaises(RuntimeError) as caught:
            await self.run_pipeline(pipeline, harness)
        self.assertIs(caught.exception, error)
        self.assertEqual((harness.audits[-1]["rows_read"], harness.audits[-1]["rows_written"]), (2, 1))
        self.assertNotIn("batch:2", harness.events)

    async def test_audit_errors(self):
        for state in ("running", "success", "failed"):
            with self.subTest(state=state):
                harness = Harness([[make_source(DimDevicePipeline.extractor.model)]])
                harness.failed_states.add(state)
                if state == "failed":
                    error = ValueError("original error")
                    harness.query_error = error
                    with self.assertLogs("src.pipelines._pipeline", level="ERROR") as logs:
                        with self.assertRaises(ValueError) as caught:
                            await self.run_pipeline(DimDevicePipeline, harness)
                    self.assertIs(caught.exception, error)
                    self.assertIn("Failed to write audit", logs.output[0])
                else:
                    with self.assertRaisesRegex(RuntimeError, f"audit {state} failed"):
                        await self.run_pipeline(DimDevicePipeline, harness)
                    expected = ["running", "failed"] if state == "running" else ["running", "success", "failed"]
                    self.assertEqual([row["state"] for row in harness.audits], expected)
                    if state == "running":
                        self.assertNotIn("oltp_open", harness.events)
                harness.client.close.assert_awaited_once()

    async def test_clickhouse_open_failure_has_no_audit(self):
        harness = Harness([])
        error = RuntimeError("cannot connect")
        harness.dwh_error = error
        with self.assertRaises(RuntimeError) as caught:
            await self.run_pipeline(DimDevicePipeline, harness)
        self.assertIs(caught.exception, error)
        harness.client.insert.assert_not_awaited()
        harness.client.close.assert_not_awaited()
        self.assertNotIn("oltp_open", harness.events)

    async def test_defaults_are_created_at_runtime(self):
        harness = Harness([])
        settings = oltp_settings()
        with patch("src.pipelines._pipeline.OltpSettings", return_value=settings) as factory:
            await self.run_pipeline(DimDevicePipeline, harness, oltp_settings=None, dwh_settings=None)
        factory.assert_called_once_with()
        self.assertIs(harness.received_oltp_settings, settings)
        self.assertIsNone(harness.received_dwh_settings)

    async def test_parallel_calls_have_independent_loads_and_connections(self):
        harnesses = [Harness([[make_source(DimDevicePipeline.extractor.model, index)]]) for index in range(2)]
        oltp_options = [oltp_settings(), oltp_settings()]
        dwh_options = [dwh_settings(), dwh_settings()]
        by_oltp = {id(option): harness for option, harness in zip(oltp_options, harnesses, strict=True)}
        by_dwh = {id(option): harness for option, harness in zip(dwh_options, harnesses, strict=True)}
        for harness in harnesses:
            async def query(*, query, parameters, current=harness):
                await asyncio.sleep(0)
                return await current.query(query=query, parameters=parameters)
            harness.client.query.side_effect = query
        with (
            patch("src.pipelines._pipeline.DwhConnector", side_effect=lambda settings: by_dwh[id(settings)].dwh_context(settings)),
            patch("src.pipelines._pipeline.OltpConnector", side_effect=lambda settings: by_oltp[id(settings)].oltp_context(settings)),
        ):
            audits = await asyncio.gather(*[
                DimDevicePipeline.run(timestamp_start=START, timestamp_end=END, dag_id="dag", run_id="same_run", task_id=f"task_{index}", oltp_settings=oltp_options[index], dwh_settings=dwh_options[index])
                for index in range(2)
            ])
        self.assertNotEqual(audits[0].load_id, audits[1].load_id)
        for harness, audit in zip(harnesses, audits, strict=True):
            self.assertEqual({row["load_id"] for row in harness.audits}, {audit.load_id})
            harness.client.close.assert_awaited_once()
            harness.session.close.assert_awaited_once()

    def test_import_does_not_construct_settings(self):
        root = Path(__file__).resolve().parents[1]
        script = "from unittest.mock import patch\nwith patch('pydantic_settings.BaseSettings.__init__', side_effect=AssertionError('settings on import')):\n    from src import pipelines\nassert len(pipelines.__all__) == 12\n"
        result = subprocess.run([sys.executable, "-B", "-c", script], cwd=root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
