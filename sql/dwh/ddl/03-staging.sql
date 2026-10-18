CREATE TABLE IF NOT EXISTS music_dwh.stg_datasets ON CLUSTER company_cluster
(
    source LowCardinality(String),
    dag_id String,
    run_id String,
    task_id String,
    interval_start DateTime64(6, 'UTC'),
    interval_end DateTime64(6, 'UTC'),
    dataset_id UUID,
    created_at DateTime64(6, 'UTC'),
    batch_count UInt64,
    row_count UInt64,
    dimension_counts Map(String, UInt64),
    topic Nullable(String),
    partition Nullable(Int32),
    committed_offset Nullable(Int64),
    first_offset Nullable(Int64),
    next_offset Nullable(Int64),
    data_expires_at Nullable(DateTime('UTC'))
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/stg_datasets', '{replica}')
ORDER BY dataset_id;

CREATE TABLE IF NOT EXISTS music_dwh.stg_datasets_all ON CLUSTER company_cluster
AS music_dwh.stg_datasets
ENGINE = Distributed(company_cluster, music_dwh, stg_datasets, cityHash64(dataset_id));

CREATE TABLE IF NOT EXISTS music_dwh.stg_batches ON CLUSTER company_cluster
(
    dataset_id UUID,
    batch_number UInt64,
    payload String,
    row_count UInt64,
    next_offset Nullable(Int64),
    stored_at DateTime('UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/stg_batches', '{replica}')
PARTITION BY toDate(stored_at)
ORDER BY (dataset_id, batch_number)
TTL stored_at + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS music_dwh.stg_batches_all ON CLUSTER company_cluster
AS music_dwh.stg_batches
ENGINE = Distributed(company_cluster, music_dwh, stg_batches, cityHash64(dataset_id));

CREATE TABLE IF NOT EXISTS music_dwh.stg_missing_dimension_ids ON CLUSTER company_cluster
(
    dataset_id UUID,
    dimension LowCardinality(String),
    entity_id UUID,
    stored_at DateTime('UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{shard}/music_dwh/stg_missing_dimension_ids', '{replica}')
PARTITION BY toDate(stored_at)
ORDER BY (dataset_id, dimension, entity_id)
TTL stored_at + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS music_dwh.stg_missing_dimension_ids_all ON CLUSTER company_cluster
AS music_dwh.stg_missing_dimension_ids
ENGINE = Distributed(company_cluster, music_dwh, stg_missing_dimension_ids, cityHash64(dataset_id));
