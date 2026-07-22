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

# Optional: install faultstorm package to get process_freezer scripts.
ARG FAULTSTORM_REPO=""
ARG FAULTSTORM_COMMIT=unknown
RUN if [ -n "$FAULTSTORM_REPO" ]; then \
        pip3 install --timeout 120 --retries 3 "${FAULTSTORM_REPO}#egg=faultstorm[postgres]" && \
        SCRIPTS_DIR=$(python3 -c "from faultstorm.scripts import get_script_path; import os; print(os.path.dirname(get_script_path('process_freezer.sh')))") && \
        cp "$SCRIPTS_DIR/process_freezer.sh" /usr/local/bin/process_freezer.sh && \
        chmod +x /usr/local/bin/process_freezer.sh && \
        cp "$SCRIPTS_DIR/process_freezer.supervisor.conf" /etc/supervisor/conf.d/process_freezer.conf; \
    fi
