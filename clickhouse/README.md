# Clickhouse Cluster

Clickhouse cluster with 2 shards and 2 replicas built with docker-compose.

Not for production use.

## Run

From the repository root, create `clickhouse/.env` from `.env.example` if it
does not exist, then set `CLICKHOUSE_ADMIN_PASSWORD`. Start the prepared configs:
```sh
make -C clickhouse up
```

Or run Compose directly from the repository root:

```sh
docker compose --env-file clickhouse/.env -f clickhouse/docker-compose.yml up -d
```

The image is `clickhouse/clickhouse-server:26.8-alpine`. Configs and users are
mounted as individual read-only files; the image's `config.d` / `users.d`
directories remain available. XML manages users, so Docker user setup is disabled.

`make -C clickhouse config` regenerates node configs from `config.xml` and
`users.xml` (requires `envsubst`) and overwrites node-specific config edits.

## Schema initialization

`clickhouse-init` waits for all four ClickHouse nodes to become healthy, connects
to `clickhouse01` as `admin`, applies `sql/dwh/ddl/01-create.sql`, then exits.
`ON CLUSTER company_cluster` distributes the DDL to all nodes. Exit code 0 means
success; any SQL error stops initialization with a non-zero exit code.

```sh
docker compose --env-file clickhouse/.env -f clickhouse/docker-compose.yml logs clickhouse-init
```

To reapply DDL after changes, with the cluster already running:

```sh
make -C clickhouse init
```

This is not a migration runner: `CREATE IF NOT EXISTS` preserves existing tables
and does not update their definitions. The `02-constraints.sql` and
`03-indexes.sql` PostgreSQL transaction placeholders are intentionally not mounted.
Additional ClickHouse-compatible scripts can be mounted in `/init/sql/`; they run
in filename order. Initialization has no persistent "already ran" marker, so scripts
must be safe to reapply.

## Persistent data

The default bind mounts store data on the host separately from configs:

- `clickhouse/data/clickhouse01` ... `clickhouse/data/clickhouse04`: `/var/lib/clickhouse`.
- `clickhouse/data/zookeeper/data`: ZooKeeper snapshots.
- `clickhouse/data/zookeeper/datalog`: ZooKeeper transaction logs.

`CLICKHOUSE_DATA_FOLDER` and `CLICKHOUSE_CONFIGS_FOLDER` are relative to the
Compose file. Data and `.env` are excluded from Git. `down` does not remove
these bind-mounted data directories.

Changing mounts does not migrate existing data from Docker volumes. Back up and
copy the old ClickHouse and ZooKeeper data before switching a populated cluster.
Do not combine old ZooKeeper metadata with empty ClickHouse data directories.

Containers will be available in docker network `172.23.0.0/24`

| Container    | Address
| ------------ | -------
| zookeeper    | 172.23.0.10
| clickhouse01 | 172.23.0.11
| clickhouse02 | 172.23.0.12
| clickhouse03 | 172.23.0.13
| clickhouse04 | 172.23.0.14

## Users

- `default`: no password, accessible only from inside the container over loopback.
- `admin`: password from `CLICKHOUSE_ADMIN_PASSWORD`, also used for inter-node queries.
- Use the same admin password in ETL/client settings. `.env.example` is a template,
  not a production secret. Change the existing development password in `.env`.

## Test it

Login to clickhouse01 console (first node's ports are mapped to localhost)
```sh
clickhouse-client -h localhost --user admin --password
```

Or open `clickhouse-client` inside any container
```sh
docker exec -it clickhouse01 clickhouse-client -h localhost --user admin --password
```

Create a test database and table (sharded and replicated)
```sql
CREATE DATABASE company_db ON CLUSTER 'company_cluster';

CREATE TABLE company_db.events ON CLUSTER 'company_cluster' (
    time DateTime,
    uid  Int64,
    type LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/events', '{replica}')
PARTITION BY toDate(time)
ORDER BY (uid);

CREATE TABLE company_db.events_distr ON CLUSTER 'company_cluster' AS company_db.events
ENGINE = Distributed('company_cluster', company_db, events, uid);
```

Load some data
```sql
INSERT INTO company_db.events_distr VALUES
    ('2020-01-01 10:00:00', 100, 'view'),
    ('2020-01-01 10:05:00', 101, 'view'),
    ('2020-01-01 11:00:00', 100, 'contact'),
    ('2020-01-01 12:10:00', 101, 'view'),
    ('2020-01-02 08:10:00', 100, 'view'),
    ('2020-01-03 13:00:00', 103, 'view');
```

Check data from the current shard
```sql
SELECT * FROM company_db.events;
```

Check data from all cluster
```sql
SELECT * FROM company_db.events_distr;
```

## Add more nodes

If you need more Clickhouse nodes, add them like this:

1. Add replicas/shards to `config.xml` to the block `clickhouse/remote_servers/company_cluster`.
1. Add nodes to `docker-compose.yml`.
1. Add nodes in `Makefile` in `config` target.

## Start, stop

Start/stop the cluster without removing containers
```sh
make -C clickhouse start
make -C clickhouse stop
```

## Teardown

Stop and remove containers
```sh
make -C clickhouse down
```
