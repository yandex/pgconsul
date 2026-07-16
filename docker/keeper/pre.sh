#!/bin/bash

# This script sets the server_id for ClickHouse Keeper based on the container's hostname.
# The server_id corresponds to the ZooKeeper myid file equivalent.
#
# Hostname patterns and their corresponding server IDs:
#   zookeeper1 -> server_id = 1
#   zookeeper2 -> server_id = 2
#   zookeeper3 -> server_id = 3
#
# This approach is more reliable than IP-based detection because Docker assigns
# IPs dynamically and they may not match the expected .10/.11/.12 addresses.

CONFIG_FILE="/etc/clickhouse-keeper/keeper_config.xml"

# Extract server_id from hostname
# Pattern: zookeeper{N} -> N
HOSTNAME=$(hostname)
if [[ "$HOSTNAME" =~ zookeeper([0-9]+) ]]; then
    SERVER_ID="${BASH_REMATCH[1]}"
elif [[ "$HOSTNAME" =~ ([0-9]+)$ ]]; then
    # Fallback: use trailing number in hostname
    SERVER_ID="${BASH_REMATCH[1]}"
else
    echo "ERROR: Could not determine server_id from hostname '$HOSTNAME'"
    echo "Expected hostname pattern: zookeeper1, zookeeper2, zookeeper3"
    exit 1
fi

if [ -n "$SERVER_ID" ] && [ "$SERVER_ID" -ge 1 ] && [ "$SERVER_ID" -le 9 ]; then
    echo "Detected server_id: $SERVER_ID for hostname: $HOSTNAME"

    # Update the server_id in the XML config file
    sed -i "s|<server_id>[0-9]*</server_id>|<server_id>${SERVER_ID}</server_id>|g" "$CONFIG_FILE"

    echo "Updated $CONFIG_FILE with server_id=$SERVER_ID"
else
    echo "ERROR: Invalid server_id '$SERVER_ID' from hostname '$HOSTNAME'"
    exit 1
fi

# Generate SSL certificates at runtime (when hostname is known)
# This is necessary because Docker sets the hostname at container start,
# not during image build.
if [ ! -f /etc/zk-ssl/server.crt ] || [ ! -f /etc/zk-ssl/server.key ]; then
    echo "Generating SSL certificates for hostname: $HOSTNAME"

    # Get FQDN (hostname -f may not work in all containers, so try domainname command)
    DOMAINNAME=$(domainname 2>/dev/null || true)
    if [ -n "$DOMAINNAME" ] && [ "$DOMAINNAME" != "(none)" ] && [ "$DOMAINNAME" != "localhost" ]; then
        FQDN="${HOSTNAME}.${DOMAINNAME}"
    else
        FQDN="$HOSTNAME"
    fi

    echo "FQDN for certificate CN: $FQDN"

    # Generate server key (without password - ClickHouse Keeper cannot read encrypted keys)
    openssl genrsa -out /etc/zk-ssl/server.key 4096

    # Generate CSR with CN=FQDN and SAN entries for both hostname and FQDN
    # SAN (Subject Alternative Name) ensures the certificate is valid for
    # both short hostname and FQDN
    cat > /tmp/openssl-san.cnf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = NL
ST = Test
L = Test
O = Test
OU = Test
CN = ${FQDN}

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${FQDN}
DNS.2 = ${HOSTNAME}
DNS.3 = localhost
DNS.4 = *.pgconsul_pgconsul_net
DNS.5 = pgconsul_zookeeper1_1.pgconsul_pgconsul_net
DNS.6 = pgconsul_zookeeper2_1.pgconsul_pgconsul_net
DNS.7 = pgconsul_zookeeper3_1.pgconsul_pgconsul_net
IP.1 = 127.0.0.1
IP.2 = 192.168.233.10
IP.3 = 192.168.233.11
IP.4 = 192.168.233.12
EOF

    openssl req -new -key /etc/zk-ssl/server.key -out /etc/zk-ssl/server.csr \
        -config /tmp/openssl-san.cnf

    openssl x509 -req -days 365 -in /etc/zk-ssl/server.csr \
        -CA /etc/zk-ssl/ca.cert.pem -CAkey /etc/zk-ssl/ca.key -CAcreateserial \
        -out /etc/zk-ssl/server.crt \
        -extensions v3_req -extfile /tmp/openssl-san.cnf

    chmod 644 /etc/zk-ssl/server.crt /etc/zk-ssl/ca.cert.pem
    chmod 600 /etc/zk-ssl/server.key /etc/zk-ssl/ca.key
    chown clickhouse:clickhouse /etc/zk-ssl/server.crt /etc/zk-ssl/server.key \
        /etc/zk-ssl/ca.cert.pem /etc/zk-ssl/ca.key

    rm -f /tmp/openssl-san.cnf
    echo "SSL certificates generated successfully"
else
    echo "SSL certificates already exist, skipping generation"
fi

# Start sshd for Jepsen test access
/usr/sbin/sshd

# Execute clickhouse-keeper as clickhouse user to match data directory ownership
exec su - clickhouse -c "clickhouse-keeper --config-file=/etc/clickhouse-keeper/keeper_config.xml"
