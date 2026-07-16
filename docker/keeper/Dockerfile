FROM clickhouse/clickhouse-keeper:26.5

# Install openssl and openssh for certificate generation and Jepsen SSH access
RUN apk add --no-cache openssl openssh bash iptables

# Configure sshd
RUN ssh-keygen -A && \
    echo "root:root" | chpasswd && \
    mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config && \
    echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config && \
    echo 'PubkeyAuthentication yes' >> /etc/ssh/sshd_config && \
    echo 'HostKeyAlgorithms +ssh-rsa' >> /etc/ssh/sshd_config && \
    echo 'PubkeyAcceptedKeyTypes +ssh-rsa' >> /etc/ssh/sshd_config

# Copy Jepsen test SSH public key for passwordless auth
COPY test_ssh_key.pub /root/.ssh/authorized_keys
RUN chmod 600 /root/.ssh/authorized_keys

# Copy pre-startup script (handles server_id + SSL cert generation at runtime)
COPY docker/keeper/pre.sh /opt/clickhouse-keeper/pre.sh
RUN chmod +x /opt/clickhouse-keeper/pre.sh

# Copy SSL certificate generation script (contains embedded CA cert/key)
COPY docker/keeper/generate_certs.sh /opt/clickhouse-keeper/generate_certs.sh
RUN chmod +x /opt/clickhouse-keeper/generate_certs.sh

# Create log directory
RUN mkdir -p /var/log/clickhouse-keeper && chown clickhouse:clickhouse /var/log/clickhouse-keeper

# Create data directories and set ownership
RUN mkdir -p /var/lib/clickhouse-keeper/coordination/log \
             /var/lib/clickhouse-keeper/coordination/snapshots \
             /var/lib/clickhouse-keeper/coordination/preprocessed_configs \
             /etc/clickhouse-keeper \
             /etc/zk-ssl && \
    chown -R clickhouse:clickhouse /var/lib/clickhouse-keeper

# Copy keeper config
COPY docker/keeper/keeper_config.xml /etc/clickhouse-keeper/keeper_config.xml

# Generate CA certificate and key at build time (embedded in generate_certs.sh)
# Server certificates are generated at runtime in pre.sh when hostname is known
RUN /opt/clickhouse-keeper/generate_certs.sh && \
    chown -R clickhouse:clickhouse /etc/zk-ssl

# Override base image ENTRYPOINT to run our pre.sh
ENTRYPOINT ["/opt/clickhouse-keeper/pre.sh"]

# Run clickhouse-keeper with config
CMD ["clickhouse-keeper", "--config-file=/etc/clickhouse-keeper/keeper_config.xml"]
