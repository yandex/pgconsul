FROM pgconsulbase:latest
ARG FAULTSTORM_REPO
RUN cp /var/lib/dist/test_ssh_key /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa && \
    apt-get -qq update && \
    apt-get install -qq --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        lsb-release && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        > /etc/apt/sources.list.d/docker.list && \
    apt-get -qq update && \
    apt-get install -qq --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*
# Cache-bust: when FAULTSTORM_COMMIT changes, pip install is re-run
ARG FAULTSTORM_COMMIT=unknown
RUN pip3 install ${FAULTSTORM_REPO}
COPY main.py /root/main.py
COPY faultstorm_config.py /root/faultstorm_config.py
COPY faultstorm_pg_client.py /root/faultstorm_pg_client.py
COPY faultstorm_switchover.py /root/faultstorm_switchover.py
