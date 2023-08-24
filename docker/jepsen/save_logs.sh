#!/bin/bash

for i in 1 2 3
do
    mkdir -p logs/postgresql${i}
    mkdir -p logs/zookeeper${i}
    for service in pgbouncer pgconsul
    do
        docker exec pgconsul_postgresql${i}_1 cat \
            /var/log/${service}.log > \
            logs/postgresql${i}/${service}.log
    done
    docker exec pgconsul_postgresql${i}_1 cat \
        /var/log/postgresql/postgresql-$1-main.log > \
        logs/postgresql${i}/postgresql.log
    docker exec pgconsul_zookeeper${i}_1 cat \
        /var/log/zookeeper/zookeeper--server-pgconsul_zookeeper${i}_1.log > \
        logs/zookeeper${i}/zk.log 2>&1
done
