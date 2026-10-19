import json
from dataclasses import asdict, fields
from datetime import UTC, datetime
from uuid import UUID

import pytest
from src.loaders import StgBatchLoader, StgDatasetLoader, StgMissingDimensionIdLoader
from src.models.dwh import (
    ClickHouseModel,
    DatasetRef,
    StgBatch,
    StgDataset,
    StgMissingDimensionId,
)
from src.staging import Dataset
from src.staging import DatasetRef as StagingDatasetRef
from src.staging.repository import Dataset as RepositoryDataset
from src.staging.repository import DatasetRef as RepositoryDatasetRef

NOW = datetime(2026, 10, 19, 12, 0, 0, 123456, tzinfo=UTC)
DATASET_VALUES = {
    "source": "fct_events",
    "dag_id": "test_dag",
    "run_id": "test_run",
    "task_id": "test_stage",
    "interval_start": NOW,
    "interval_end": NOW,
}


@pytest.mark.parametrize(
    "model, loader, table, values",
    [
        (
            StgDataset,
            StgDatasetLoader,
            "stg_datasets",
            {
                **DATASET_VALUES,
                "dataset_id": UUID(int=1),
                "created_at": NOW,
                "batch_count": 0,
                "row_count": 0,
                "dimension_counts": {"track": 0},
                "topic": None,
                "partition": None,
                "committed_offset": None,
                "first_offset": None,
                "next_offset": None,
                "data_expires_at": None,
            },
        ),
        (
            StgBatch,
            StgBatchLoader,
            "stg_batches",
            {
                "dataset_id": UUID(int=1),
                "batch_number": 0,
                "payload": '[{"name": "Music 🎵"}]',
                "row_count": 1,
                "next_offset": None,
                "stored_at": NOW,
            },
        ),
        (
            StgMissingDimensionId,
            StgMissingDimensionIdLoader,
            "stg_missing_dimension_ids",
            {
                "dataset_id": UUID(int=1),
                "dimension": "track",
                "entity_id": UUID(int=2),
                "stored_at": NOW,
            },
        ),
    ],
)
def test_staging_models_positive_columns_and_rows(model, loader, table, values):
    """[unit] Staging models expose exactly the DDL columns and typed loader targets."""
    entity = model(**values)
    assert isinstance(entity, ClickHouseModel)
    assert asdict(entity) == values
    assert model.column_names() == tuple(values)
    assert tuple(field.name for field in fields(model)) == tuple(values)
    assert entity.to_row() == tuple(values.values())
    assert model.table_name == f"music_dwh.{table}"
    assert model.distributed_table_name == f"music_dwh.{table}_all"
    assert model.table_to_insert() == f"music_dwh.{table}_all"
    assert loader.model is model
    with pytest.raises(TypeError):
        model(*values.values())
    with pytest.raises(AttributeError):
        entity.unexpected_field = True


def test_stg_dataset_positive_defaults_and_compatibility():
    """[unit] Dataset aliases preserve defaults, independent dictionaries and XCom format."""
    assert Dataset is RepositoryDataset is StgDataset
    assert DatasetRef is StagingDatasetRef is RepositoryDatasetRef
    first = Dataset(**DATASET_VALUES)
    second = StgDataset(**DATASET_VALUES)
    assert first.dataset_id != second.dataset_id
    assert first.dataset_id.version == second.dataset_id.version == 7
    assert first.created_at.utcoffset() is not None
    assert first.batch_count == first.row_count == 0
    assert first.dimension_counts == second.dimension_counts == {}
    first.dimension_counts["track"] = 1
    assert second.dimension_counts == {}
    assert all(
        getattr(first, name) is None
        for name in (
            "topic",
            "partition",
            "committed_offset",
            "first_offset",
            "next_offset",
            "data_expires_at",
        )
    )
    assert json.loads(json.dumps(first.reference())) == {
        "dataset_id": str(first.dataset_id)
    }


def test_stg_batch_positive_default_offset():
    """[unit] Batch offset defaults to null while an explicit zero stays unchanged."""
    values = {
        "dataset_id": UUID(int=1),
        "batch_number": 0,
        "payload": "[]",
        "row_count": 0,
        "stored_at": NOW,
    }
    assert StgBatch(**values).next_offset is None
    assert StgBatch(**values, next_offset=0).next_offset == 0
