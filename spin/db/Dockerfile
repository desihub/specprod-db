FROM postgres:16

RUN apt-get update && \
    apt-get upgrade && \
    apt-get install -y --no-install-recommends procps vim postgresql-16-q3c

COPY --chown=postgres:postgres *.sh /docker-entrypoint-initdb.d/
