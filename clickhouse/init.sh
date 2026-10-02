#!/bin/sh
set -eu

: "${CLICKHOUSE_ADMIN_PASSWORD:?Set CLICKHOUSE_ADMIN_PASSWORD in clickhouse/.env}"

# Credentials are read by clickhouse-client from the environment, not printed
# or passed as command-line arguments.
export CLICKHOUSE_USER=admin
export CLICKHOUSE_PASSWORD="$CLICKHOUSE_ADMIN_PASSWORD"

sql_dir="${CLICKHOUSE_INIT_SQL_DIR:-/init/sql}"
set -- "$sql_dir"/*.sql
if [ ! -f "$1" ]; then
    printf 'No SQL scripts found in %s\n' "$sql_dir" >&2
    exit 1
fi

for sql_file do
    printf 'Applying %s\n' "${sql_file##*/}"
    clickhouse-client \
        --host clickhouse01 \
        --port 9000 \
        --multiquery \
        --distributed_ddl_task_timeout 180 \
        --distributed_ddl_output_mode throw \
        --queries-file "$sql_file"
done

printf 'ClickHouse schema initialization completed.\n'
