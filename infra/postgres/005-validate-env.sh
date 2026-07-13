#!/bin/sh
set -eu

require_identifier() {
  name=$1
  value=$2
  case "$value" in
    ""|[0-9]*|*[!a-z0-9_]*)
      printf '%s must be a lowercase PostgreSQL identifier\n' "$name" >&2
      exit 1
      ;;
  esac
}

require_secret() {
  name=$1
  value=$2
  case "$value" in
    *[Cc][Hh][Aa][Nn][Gg][Ee][Mm][Ee]*)
      printf '%s contains a development placeholder\n' "$name" >&2
      exit 1
      ;;
  esac
  if [ "${#value}" -lt 16 ]; then
    printf '%s must contain at least 16 characters\n' "$name" >&2
    exit 1
  fi
}

: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
: "${DB_MIGRATOR_USER:?DB_MIGRATOR_USER is required}"
: "${DB_MIGRATOR_PASSWORD:?DB_MIGRATOR_PASSWORD is required}"
: "${DB_RUNTIME_USER:?DB_RUNTIME_USER is required}"
: "${DB_RUNTIME_PASSWORD:?DB_RUNTIME_PASSWORD is required}"

data_directory=${PGDATA:-/var/lib/postgresql/data}
bootstrap_marker="$data_directory/.bazaar-guru-bootstrap-complete"
if [ -s "$data_directory/PG_VERSION" ] && [ ! -f "$bootstrap_marker" ]; then
  printf 'existing PostgreSQL data lacks Bazaar Guru bootstrap marker\n' >&2
  exit 1
fi

require_identifier POSTGRES_DB "$POSTGRES_DB"
require_identifier POSTGRES_USER "$POSTGRES_USER"
require_identifier DB_MIGRATOR_USER "$DB_MIGRATOR_USER"
require_identifier DB_RUNTIME_USER "$DB_RUNTIME_USER"
require_secret POSTGRES_PASSWORD "$POSTGRES_PASSWORD"
require_secret DB_MIGRATOR_PASSWORD "$DB_MIGRATOR_PASSWORD"
require_secret DB_RUNTIME_PASSWORD "$DB_RUNTIME_PASSWORD"

if [ "$POSTGRES_USER" = "$DB_MIGRATOR_USER" ] || \
   [ "$POSTGRES_USER" = "$DB_RUNTIME_USER" ] || \
   [ "$DB_MIGRATOR_USER" = "$DB_RUNTIME_USER" ]; then
  printf 'bootstrap, migrator, and runtime roles must be distinct\n' >&2
  exit 1
fi

if [ "$POSTGRES_PASSWORD" = "$DB_MIGRATOR_PASSWORD" ] || \
   [ "$POSTGRES_PASSWORD" = "$DB_RUNTIME_PASSWORD" ] || \
   [ "$DB_MIGRATOR_PASSWORD" = "$DB_RUNTIME_PASSWORD" ]; then
  printf 'bootstrap, migrator, and runtime passwords must be distinct\n' >&2
  exit 1
fi

exec docker-entrypoint.sh "$@"
