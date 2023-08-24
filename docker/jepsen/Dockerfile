FROM pgconsulbase:latest
ENV LEIN_ROOT 1
RUN apt-get -qq update && apt-get install libjna-java \
        gnuplot \
        wget && \
    cp /var/lib/dist/test_ssh_key /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa && \
    wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein -O /usr/bin/lein && \
    chmod +x /usr/bin/lein && \
    cp -r /var/lib/dist/docker/jepsen/jepsen /root/ && \
    cd /root/jepsen && \
    lein install && \
    lein deps
