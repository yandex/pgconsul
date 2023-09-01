FROM pgconsulbase:latest
ARG replication_type
ARG pg_major
ARG pg_version
ARG pg_common_version
ARG pgbouncer_version
ARG libpq_version

ENV PG_MAJOR $pg_major

RUN apt-get update && \
    apt-get install \
        libpq5 \
        postgresql-$PG_MAJOR \
        postgresql-client-$PG_MAJOR \
        postgresql-common \
        postgresql-client-common \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        python-daemon \
        python-psycopg2 \
        python-setuptools \
        python-kazoo \
        pgbouncer

RUN git clone https://github.com/g0djan/lwaldump.git lwaldump && \
    cd lwaldump && git checkout REL_13_STABLE && make -s && make -s install && cd ..

RUN pg_dropcluster --stop $PG_MAJOR main && \
    ln -s /usr/lib/postgresql/$PG_MAJOR/bin /usr/bin/postgresql && \
    mkdir -p /etc/pgconsul/plugins && \
    cd /var/lib/dist && \
    DESTDIR=/ make install && \
    cp /var/lib/dist/docker/pgconsul/pgconsul_${replication_type}.conf /etc/pgconsul.conf && \
    cp /var/lib/dist/docker/pgconsul/gen_rec_conf.sh /usr/local/bin/gen_rec_conf.sh && \
    echo "*:*:*:repl:repl" > /var/lib/postgresql/.pgpass && \
    chmod 600 /var/lib/postgresql/.pgpass && \
    chown postgres:postgres /var/lib/postgresql/.pgpass && \
    echo "*:*:*:repl:repl" > /root/.pgpass && \
    chmod 600 /root/.pgpass && \
    mkdir -p /etc/pgbouncer && \
    cp /var/lib/dist/docker/pgconsul/postgresql.conf /root/postgresql.conf && \
    cp /var/lib/dist/docker/pgconsul/pg_hba.conf /root/pg_hba.conf && \
    cp /var/lib/dist/docker/pgconsul/supervisor.conf /etc/supervisor/conf.d/pgconsul.conf && \
    cp /var/lib/dist/docker/pgconsul/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini && \
    cp /var/lib/dist/docker/pgconsul/userlist.txt /etc/pgbouncer/userlist.txt && \
    cp /var/lib/dist/docker/pgconsul/sudoers /etc/sudoers.d/pgconsul && \
    cp /var/lib/dist/docker/pgconsul/setup.sh /usr/local/bin/setup.sh && \
    cp /var/lib/dist/docker/pgconsul/archive.passwd /etc/archive.passwd && \
    chown postgres:postgres /etc/archive.passwd && \
    chmod 600 /etc/archive.passwd

