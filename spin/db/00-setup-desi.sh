#!/bin/bash
set -o errexit;

[[ -n "${DESI_ADMIN_PASSWORD}" && -n "${DESI_PASSWORD}" ]]

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    CREATE USER desi_admin WITH CREATEDB ENCRYPTED PASSWORD '$(<${DESI_ADMIN_PASSWORD})';
    CREATE USER desi WITH ENCRYPTED PASSWORD '$(<${DESI_PASSWORD})';
    CREATE DATABASE desi WITH OWNER desi_admin;
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname desi -c 'CREATE EXTENSION q3c;'

