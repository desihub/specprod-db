FROM postgres:11-bullseye

RUN apt-get update && apt-get upgrade && apt-get install -y --no-install-recommends postgresql-11-q3c

COPY --chown=postgres:postgres *.sh /docker-entrypoint-initdb.d/
