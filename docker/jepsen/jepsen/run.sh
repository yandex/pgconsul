#!/bin/sh

set -e
set -x

cd "$(dirname "$0")"
export LEIN_ROOT=1
for i in zookeeper1 zookeeper2 zookeeper3 postgresql1 postgresql2 postgresql3
do
    ssh-keyscan -t rsa pgconsul_${i}_1.pgconsul_pgconsul_net >> /root/.ssh/known_hosts
done
lein test
