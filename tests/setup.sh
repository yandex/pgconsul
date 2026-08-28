#!/bin/bash

PG_MAJOR=$1

sudo -u postgres /usr/lib/postgresql/$PG_MAJOR/bin/postgres --single -D /var/lib/postgresql/$PG_MAJOR/main <<- EOF
CREATE EXTENSION IF NOT EXISTS lwaldump;
EOF
