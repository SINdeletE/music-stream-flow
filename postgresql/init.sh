#!/bin/sh
set -eu

: "${PGHOST:?Set PGHOST to the PostgreSQL service name}"
: "${PGUSER:?Set PGUSER from MUSIC_OLTP_USERNAME}"
: "${PGDATABASE:?Set PGDATABASE from MUSIC_OLTP_DATABASE}"
: "${PGPASSWORD:?Set PGPASSWORD from MUSIC_OLTP_PASSWORD}"

# libpq reads credentials from the environment, not command-line arguments.
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-5}"

sql_dir="${POSTGRES_INIT_SQL_DIR:-/init/sql}"
set -- "$sql_dir"/[0-9]*.sql
if [ ! -f "$1" ]; then
    printf 'No initialization SQL scripts found in %s\n' "$sql_dir" >&2
    exit 1
fi

for sql_file do
    printf 'Applying %s\n' "${sql_file##*/}"
    psql --no-psqlrc --set ON_ERROR_STOP=1 --file "$sql_file"
done

printf 'PostgreSQL schema initialization completed.\n'
