import runpy
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace


def test_staging_dag_positive_dependencies(monkeypatch):
    """[unit] DAG waits for all staging inputs, collects once, then loads dimensions, bridges and facts."""
    nodes = {}

    class Node:
        def __init__(self, task_id, args, kwargs):
            self.task_id = task_id
            self.parents = set()
            nodes[task_id] = self
            self.dependencies((args, kwargs))

        def dependencies(self, value):
            if isinstance(value, Node):
                self.parents.add(value.task_id)
            elif isinstance(value, dict):
                for item in value.values():
                    self.dependencies(item)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    self.dependencies(item)

        def __rshift__(self, other):
            if isinstance(other, list):
                for item in other:
                    item.parents.add(self.task_id)
            else:
                other.parents.add(self.task_id)
            return other

        def __rrshift__(self, other):
            for item in other:
                self.parents.add(item.task_id)
            return self

    class Dag:
        def __init__(self, *args, **kwargs):
            assert kwargs["max_active_runs"] == 1

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def task(*, task_id):
        def decorate(function):
            def call(*args, **kwargs):
                return Node(task_id, args, kwargs)

            return call

        return decorate

    def cross_downstream(left, right):
        for node in left:
            node >> right

    sdk = ModuleType("airflow.sdk")
    sdk.DAG = Dag
    sdk.Variable = SimpleNamespace()
    sdk.task = task
    sdk.get_current_context = dict
    sdk.cross_downstream = cross_downstream
    pendulum = ModuleType("pendulum")
    pendulum.datetime = lambda *args, tz: datetime(*args, tzinfo=UTC)
    monkeypatch.setitem(sys.modules, "airflow", ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk)
    monkeypatch.setitem(sys.modules, "pendulum", pendulum)
    monkeypatch.setattr(
        "src.settings.KafkaSettings",
        lambda: SimpleNamespace(music_kafka_num_partitions=3),
    )
    module = runpy.run_path(str(Path(__file__).resolve().parents[3] / "etl_msf.py"))
    assert isinstance(module["KAFKA_EVENTS_TASK_IDS"], tuple)
    stages = {name for name in nodes if name.startswith("stage_")}
    dims = {task_id for task_id, _ in module["DIM_PIPELINES"]}
    bridges = {task_id for task_id, _, _ in module["BRG_PIPELINES"]}
    facts = {f"load_kafka_events_partition_{i}" for i in range(3)}
    assert len(stages) == 8
    for name in stages:
        assert nodes[name].parents == {"fix_timestamp"}
    assert nodes["collect"].parents == stages | {"fix_timestamp"}
    for name in dims:
        assert nodes[name].parents == {"fix_timestamp", "collect"}
    for name in bridges:
        assert nodes[name].parents == dims | {
            "fix_timestamp",
            name.replace("load_", "stage_", 1),
        }
    for name in facts:
        assert nodes[name].parents == bridges | {
            "fix_timestamp",
            name.replace("load_", "stage_", 1),
        }
    assert nodes["swap_timestamp"].parents == facts | {"fix_timestamp"}
