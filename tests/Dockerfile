FROM pgconsulbase:latest

ARG pg_major
ARG confpath="tests/conf"

ENV PG_MAJOR=$pg_major
ENV pgconsul_CONFIG=/etc/pgconsul.conf
ENV PGBOUNCER_CONFIG=/etc/pgbouncer/pgbouncer.ini
ENV POSTGRESQL_DATADIR=/var/lib/postgresql/$PG_MAJOR/main
ENV POSTGRESQL_CONFIG=$POSTGRESQL_DATADIR/postgresql.conf
ENV POSTGRESQL_PGHBA=$POSTGRESQL_DATADIR/pg_hba.conf

# Install PostgreSQL and additional packages for tests
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-$PG_MAJOR \
        postgresql-client-$PG_MAJOR \
        postgresql-server-dev-$PG_MAJOR && \
    rm -rf /var/lib/apt/lists/*


RUN echo "wal_level = replica" >> /etc/postgresql-common/createcluster.conf

# Build repl_mon (replication monitoring) - specific for tests
ENV USE_PGXS=1
RUN git clone https://github.com/g0djan/lwaldump.git lwaldump && \
    cd lwaldump && git checkout REL_13_STABLE && make -s && make -s install && cd .. && \
    rm -rf lwaldump && \
    git clone https://github.com/man-brain/repl_mon repl_mon && \
    cd repl_mon && make -s && make -s install && cd .. && \
    rm -rf repl_mon

# Setup python alternative
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Copy configuration files
COPY $confpath/postgresql.conf $POSTGRESQL_CONFIG
COPY $confpath/pg_hba.conf $POSTGRESQL_PGHBA
RUN chmod 666 $POSTGRESQL_PGHBA && chmod 666 $POSTGRESQL_CONFIG
RUN cp /etc/postgresql/$PG_MAJOR/main/pg_ident.conf $POSTGRESQL_DATADIR \
    && chown postgres:postgres $POSTGRESQL_DATADIR/pg_ident.conf
RUN ln -s /usr/lib/postgresql/$PG_MAJOR/bin /usr/bin/postgresql

# Initialize PostgreSQL cluster
USER postgres
RUN echo "CHECKPOINT;" | /usr/bin/postgresql/postgres --single \
        -D $POSTGRESQL_DATADIR postgres
RUN mkdir /var/lib/postgresql/$PG_MAJOR/main/conf.d
USER root

# Create directories for pgconsul and supervisor
RUN mkdir /var/log/pgconsul && chown postgres:postgres /var/log/pgconsul && \
    mkdir -p /etc/pgconsul/plugins && mkdir /var/log/supervisor && \
    mkdir -p /etc/pgbouncer

# Setup pgbouncer
RUN echo '"postgres" ""' >> /etc/pgbouncer/userlist.txt

# Copy pgconsul, pgbouncer and supervisor configuration files
COPY $confpath/pgconsul.conf $pgconsul_CONFIG
COPY $confpath/pgbouncer.ini $PGBOUNCER_CONFIG
COPY $confpath/supervisord.conf /etc/supervisor/supervisord.conf
COPY $confpath/pgbouncer.supervisor /etc/supervisor/conf.d/pgbouncer.conf
COPY $confpath/pgconsul.supervisor /etc/supervisor/conf.d/pgconsul.conf
COPY $confpath/gen_rec_conf_with_slot.sh /usr/local/bin/
COPY $confpath/gen_rec_conf_without_slot.sh /usr/local/bin/

# Setup WAL archiving
COPY $confpath/archive.passwd /etc
RUN chown postgres:postgres /etc/archive.passwd && chmod 600 /etc/archive.passwd
RUN echo "su - postgres -c '/usr/lib/postgresql/$PG_MAJOR/bin/pg_ctl -D /var/lib/postgresql/$PG_MAJOR/main/ promote'" > /usr/bin/promote && chmod +x /usr/bin/promote

# Copy and execute setup script
COPY tests/setup.sh /usr/local/bin/setup.sh
RUN chmod +x /usr/local/bin/setup.sh && /usr/local/bin/setup.sh $PG_MAJOR

# Install pgconsul from source
RUN mkdir /repo
COPY ./bin /repo/bin
COPY ./static /repo/static
COPY ./requirements.txt /repo/
COPY ./Makefile /repo/

RUN cd /repo && ls -lah
RUN cd /repo && DESTDIR=/ make install_dep

COPY ./src /repo/src
COPY ./setup.py /repo/
RUN cd /repo && DESTDIR=/ make install_pgconsul
RUN ln -s /repo/src/plugins/upload_wals.py /etc/pgconsul/plugins/upload_wals.py
RUN chown -R postgres:postgres /opt/yandex/pgconsul /etc/pgconsul/plugins /repo/src/plugins

# Generate SSL certificates at container startup
COPY tests/generate_certs.sh /usr/local/bin/generate_certs.sh
RUN chmod 755 /usr/local/bin/generate_certs.sh \
    && /usr/local/bin/generate_certs.sh

CMD ["/usr/local/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
