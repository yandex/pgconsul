FROM clickhouse/clickhouse-keeper:26.5

# Install openssl for certificate generation (runtime)
RUN apk add --no-cache openssl

# Copy pre-startup script (handles server_id + SSL cert generation at runtime)
COPY pre.sh /opt/clickhouse-keeper/pre.sh
RUN chmod +x /opt/clickhouse-keeper/pre.sh

# Copy SSL certificate generation script (contains embedded CA cert/key)
COPY generate_certs.sh /opt/clickhouse-keeper/generate_certs.sh
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
COPY keeper_config.xml /etc/clickhouse-keeper/keeper_config.xml

# Generate CA certificate and key at build time (embedded in generate_certs.sh)
# Server certificates are generated at runtime in pre.sh when hostname is known
RUN /opt/clickhouse-keeper/generate_certs.sh && \
    chown -R clickhouse:clickhouse /etc/zk-ssl

# Override base image ENTRYPOINT to run our pre.sh
ENTRYPOINT ["/opt/clickhouse-keeper/pre.sh"]

# Run clickhouse-keeper with config
CMD ["clickhouse-keeper", "--config-file=/etc/clickhouse-keeper/keeper_config.xml"]
