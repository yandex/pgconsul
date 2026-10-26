#!/bin/bash

PG_MAJOR=$1

# lwaldump is created in the public schema. Harden the postgres role's
# search_path with no public so the test cluster exercises
# pgconsul's queries under that path and catches unqualified references to
# public objects such as lwaldump().
sudo -u postgres /usr/lib/postgresql/$PG_MAJOR/bin/postgres --single -D /var/lib/postgresql/$PG_MAJOR/main <<- EOF
CREATE EXTENSION IF NOT EXISTS lwaldump;
ALTER ROLE postgres SET search_path = pg_catalog, pg_temp;
EOF
