import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


class EtlMsfDagTests(unittest.TestCase):
    def test_graph_interval_and_pipeline_calls(self):
        script = textwrap.dedent("""
            import asyncio
            import inspect
            import os
            import tempfile
            from pathlib import Path
            from unittest.mock import AsyncMock, patch

            import pendulum

            with tempfile.TemporaryDirectory(prefix="music-airflow-test-") as directory:
                os.environ["AIRFLOW_HOME"] = directory
                os.environ["AIRFLOW_CONFIG"] = str(Path(directory) / "airflow.cfg")
                os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"

                from airflow.sdk import Variable
                from src.connections import DwhSettings, OltpSettings

                with (
                    patch.object(Variable, "get", side_effect=AssertionError("Variable.get on import")),
                    patch.object(Variable, "set", side_effect=AssertionError("Variable.set on import")),
                    patch.object(OltpSettings, "__init__", side_effect=AssertionError("OLTP settings on import")),
                    patch.object(DwhSettings, "__init__", side_effect=AssertionError("DWH settings on import")),
                ):
                    from src import etl_msf

                dag = etl_msf.dag
                dims = {task_id for task_id, _ in etl_msf.DIM_PIPELINES}
                bridges = {task_id for task_id, _ in etl_msf.BRG_PIPELINES}
                assert len(dims) == 7 and len(bridges) == 5
                assert set(dag.task_ids) == dims | bridges | {"fix_timestamp", "swap_timestamp"}
                assert not dag.get_task("fix_timestamp").upstream_task_ids
                assert not dag.get_task("swap_timestamp").downstream_task_ids
                assert dag.max_active_runs == 1 and dag.catchup is False

                for task_id in dims:
                    node = dag.get_task(task_id)
                    assert node.upstream_task_ids == {"fix_timestamp"}
                    assert node.downstream_task_ids == bridges
                for task_id in bridges:
                    node = dag.get_task(task_id)
                    assert node.upstream_task_ids == dims | {"fix_timestamp"}
                    assert node.downstream_task_ids == {"swap_timestamp"}
                assert dag.get_task("swap_timestamp").upstream_task_ids == bridges | {"fix_timestamp"}

                for task_id in dims | bridges | {"swap_timestamp"}:
                    node = dag.get_task(task_id)
                    assert node.op_args[0] is etl_msf.task_timestamp_fixing
                    assert node.trigger_rule == "all_success"

                start = pendulum.datetime(2026, 10, 4, tz="Europe/Moscow")
                end = pendulum.datetime(2026, 10, 5, tz="Europe/Moscow")
                interval = {"timestamp_start": start, "timestamp_end": end}

                async def check_loads():
                    for task_id, pipeline in (*etl_msf.DIM_PIPELINES, *etl_msf.BRG_PIPELINES):
                        node = dag.get_task(task_id)
                        assert inspect.iscoroutinefunction(node.python_callable)
                        context = {"dag": dag, "run_id": "test_run", "task": node}
                        with (
                            patch("src.etl_msf.get_current_context", return_value=context),
                            patch.object(pipeline, "run", new_callable=AsyncMock) as run,
                        ):
                            await node.python_callable(interval)
                            run.assert_awaited_once_with(
                                timestamp_start=start,
                                timestamp_end=end,
                                dag_id="etl_msf",
                                run_id="test_run",
                                task_id=task_id,
                            )

                asyncio.run(check_loads())

                with (
                    patch("src.etl_msf.get_current_context", return_value={"data_interval_start": end}),
                    patch.object(Variable, "get", return_value=start.isoformat()) as get,
                ):
                    fixed = dag.get_task("fix_timestamp").python_callable()
                    get.assert_called_once_with("etl_msf_last_timestamp_end", default=etl_msf.first_date.isoformat())
                    assert fixed == interval

                with patch.object(Variable, "set") as set_variable:
                    dag.get_task("swap_timestamp").python_callable(interval)
                    set_variable.assert_called_once_with("etl_msf_last_timestamp_end", end.isoformat())

                print("14 tasks, dependency barriers and all 12 pipeline calls verified")
        """)
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=root,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
