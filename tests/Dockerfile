FROM ubuntu:jammy

ARG pg_major
ARG confpath="tests/conf"

ENV DEBIAN_FRONTEND noninteractive
ENV PG_MAJOR $pg_major
ENV pgconsul_CONFIG /etc/pgconsul.conf
ENV PGBOUNCER_CONFIG /etc/pgbouncer/pgbouncer.ini
ENV POSTGRESQL_DATADIR /var/lib/postgresql/$PG_MAJOR/main
ENV POSTGRESQL_CONFIG $POSTGRESQL_DATADIR/postgresql.conf
ENV POSTGRESQL_PGHBA $POSTGRESQL_DATADIR/pg_hba.conf

ADD https://www.postgresql.org/media/keys/ACCC4CF8.asc keyring.asc
RUN echo 'APT::Install-Recommends "0"; \n\
APT::Get::Assume-Yes "true"; \n\
APT::Get::allow-downgrades "true"; \n\
APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01buildconfig && \
    apt-get update && \
    apt-get install -qq --no-install-recommends gpg gpg-agent wget gnupg ca-certificates && \
    apt-key add keyring.asc

RUN echo "deb http://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
        apt-get update && apt-get -y install \
        postgresql-common \
        postgresql-client-common
RUN echo "wal_level = replica" >> /etc/postgresql-common/createcluster.conf

RUN apt-get -y install \
        git \
        pgbouncer \
        postgresql-$PG_MAJOR \
        postgresql-client-$PG_MAJOR \
        postgresql-server-dev-$PG_MAJOR \
        build-essential \
        python3-dev \
        python3-pip \
        python3-venv \
        python3-setuptools \
        libpq-dev \
        gcc \
        lsof \
        sudo \
        rsync

RUN git clone https://github.com/g0djan/lwaldump.git lwaldump && \
    cd lwaldump && git checkout REL_13_STABLE && make -s && make -s install && cd ..

ENV USE_PGXS=1
RUN git clone https://github.com/man-brain/repl_mon repl_mon && \
    cd repl_mon && make -s && make -s install && cd ..

RUN pip3 install \
        git+https://github.com/Supervisor/supervisor.git@4.2.5

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

COPY $confpath/postgresql.conf $POSTGRESQL_CONFIG
COPY $confpath/pg_hba.conf $POSTGRESQL_PGHBA
RUN chmod 666 $POSTGRESQL_PGHBA && chmod 666 $POSTGRESQL_CONFIG
RUN cp /etc/postgresql/$PG_MAJOR/main/pg_ident.conf $POSTGRESQL_DATADIR \
    && chown postgres:postgres $POSTGRESQL_DATADIR/pg_ident.conf
RUN ln -s /usr/lib/postgresql/$PG_MAJOR/bin /usr/bin/postgresql

USER postgres
RUN echo "CHECKPOINT;" | /usr/bin/postgresql/postgres --single \
        -D $POSTGRESQL_DATADIR postgres
RUN mkdir /var/lib/postgresql/$PG_MAJOR/main/conf.d
USER root

RUN mkdir /var/log/pgconsul && chown postgres:postgres /var/log/pgconsul && \
    mkdir -p /etc/pgconsul/plugins && mkdir /var/log/supervisor && \
    mkdir -p /etc/pgbouncer

RUN echo '"postgres" ""' >> /etc/pgbouncer/userlist.txt

COPY $confpath/pgconsul.conf $pgconsul_CONFIG
COPY $confpath/pgbouncer.ini $PGBOUNCER_CONFIG
COPY $confpath/supervisord.conf /etc/supervisor/supervisord.conf
COPY $confpath/pgbouncer.supervisor /etc/supervisor/conf.d/pgbouncer.conf
COPY $confpath/pgconsul.supervisor /etc/supervisor/conf.d/pgconsul.conf
COPY $confpath/gen_rec_conf_with_slot.sh /usr/local/bin/
COPY $confpath/gen_rec_conf_without_slot.sh /usr/local/bin/

COPY $confpath/archive.passwd /etc
RUN chown postgres:postgres /etc/archive.passwd && chmod 600 /etc/archive.passwd
RUN echo "su - postgres -c '/usr/lib/postgresql/$PG_MAJOR/bin/pg_ctl -D /var/lib/postgresql/$PG_MAJOR/main/ promote'" > /usr/bin/promote > /usr/bin/promote && chmod +x /usr/bin/promote

COPY tests/setup.sh /usr/local/bin/setup.sh

RUN chmod +x /usr/local/bin/setup.sh && /usr/local/bin/setup.sh $PG_MAJOR

CMD ["/usr/local/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]

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

COPY tests/generate_certs.sh /usr/local/bin/generate_certs.sh
RUN chmod 755 /usr/local/bin/generate_certs.sh
