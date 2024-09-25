#!/bin/bash
set -o errexit

[[ -n "${DESI_ADMIN_PASSWORD}" && \
   -n "${DESI_PASSWORD}" && \
   -n "${DESI_PUBLIC_PASSWORD}" ]]

if [[ -f "${DESI_ADMIN_PASSWORD}" ]]; then
    desi_admin=$(<${DESI_ADMIN_PASSWORD})
else
    desi_admin="${DESI_ADMIN_PASSWORD}"
fi

if [[ -f "${DESI_PASSWORD}" ]]; then
    desi=$(<${DESI_PASSWORD})
else
    desi="${DESI_PASSWORD}"
fi

if [[ -f "${DESI_PUBLIC_PASSWORD}" ]]; then
    desi_public=$(<${DESI_PUBLIC_PASSWORD})
else
    desi_public="${DESI_PUBLIC_PASSWORD}"
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    CREATE USER desi_admin WITH CREATEDB ENCRYPTED PASSWORD '${desi_admin}';
    CREATE USER desi WITH ENCRYPTED PASSWORD '${desi}';
    CREATE USER desi_public WITH ENCRYPTED PASSWORD '${desi_public}';
    CREATE DATABASE desi WITH OWNER desi_admin;
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname desi <<-EOSQLDESI
    CREATE EXTENSION q3c;
    ALTER SCHEMA public OWNER TO desi_admin;
EOSQLDESI
