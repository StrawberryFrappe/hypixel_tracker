#!/bin/sh
set -eu

: "${DB_MIGRATOR_USER:?DB_MIGRATOR_USER is required}"
: "${DB_MIGRATOR_PASSWORD:?DB_MIGRATOR_PASSWORD is required}"
: "${DB_RUNTIME_USER:?DB_RUNTIME_USER is required}"
: "${DB_RUNTIME_PASSWORD:?DB_RUNTIME_PASSWORD is required}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=bootstrap_user="$POSTGRES_USER" \
  --set=migrator_user="$DB_MIGRATOR_USER" \
  --set=migrator_password="$DB_MIGRATOR_PASSWORD" \
  --set=runtime_user="$DB_RUNTIME_USER" \
  --set=runtime_password="$DB_RUNTIME_PASSWORD" <<'SQL'
BEGIN;
SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
  :'migrator_user', :'migrator_password'
) \gexec
SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION',
  :'runtime_user', :'runtime_password'
) \gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', current_database(), :'migrator_user') \gexec
SELECT format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO %I', current_database(), :'migrator_user') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user') \gexec
SELECT format('ALTER ROLE %I NOLOGIN', :'bootstrap_user') \gexec
COMMIT;
SQL

touch "${PGDATA:-/var/lib/postgresql/data}/.bazaar-guru-bootstrap-complete"
