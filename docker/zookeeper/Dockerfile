FROM pgconsulbase:latest
ARG VERSION
ENV ZK_VERSION ${VERSION}
RUN tar -xzf /var/lib/dist/docker/zookeeper/zookeeper-${VERSION}.tar.gz -C /opt && \
    mv /opt/apache-zookeeper-${VERSION}-bin /opt/zookeeper && \
    cp /var/lib/dist/docker/zookeeper/zoo.cfg /opt/zookeeper/conf/zoo.cfg && \
    cp /var/lib/dist/docker/zookeeper/pre.sh /opt/zookeeper/bin/pre.sh && \
    chmod +x /opt/zookeeper/bin/pre.sh && \
    sed -i "s/{zk_version}/${VERSION}/g" /var/lib/dist/docker/zookeeper/zookeeper.conf && \
    cp /var/lib/dist/docker/zookeeper/zookeeper.conf /etc/supervisor/conf.d && \
    mkdir -p /var/log/zookeeper
