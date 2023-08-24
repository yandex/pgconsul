FROM ubuntu:bionic
ENV container docker
ENV DEBIAN_FRONTEND noninteractive
ADD https://www.postgresql.org/media/keys/ACCC4CF8.asc keyring.asc
RUN echo 'APT::Install-Recommends "0"; \n\
APT::Get::Assume-Yes "true"; \n\
APT::Get::allow-downgrades "true"; \n\
APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01buildconfig && \
    apt-get update && \
    apt-get install -qq --no-install-recommends gpg gpg-agent && \
    apt-key add keyring.asc

RUN echo "deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install wget gnupg ca-certificates locales && \
    locale-gen en_US.UTF-8 && \
    apt-get update && \
    apt-get install \
        openjdk-11-jre-headless \
        less \
        bind9-host \
        net-tools \
        iputils-ping \
        sudo \
        telnet \
        git \
        pgbouncer \
        python3-dev \
        python3-pip \
        python3-venv \
        python3-wheel \
        python3-setuptools \
        openssh-server \
        libpq-dev \
        gcc \
        faketime \
        rsync \
        openssl \
        iptables \
        coreutils && \
    pip3 install git+https://github.com/Supervisor/supervisor.git@4619168a4d820b37641a4719e211cf867bd7f49d && \
    pip3 install wheel && \
    rm -rf /var/run && \
    ln -s /dev/shm /var/run
COPY ./ /var/lib/dist
COPY tests/generate_certs.sh /usr/local/bin/generate_certs.sh
RUN chmod 755 /usr/local/bin/generate_certs.sh
RUN mkdir /root/.ssh && \
    chmod 700 /root/.ssh && \
    cp /var/lib/dist/test_ssh_key.pub /root/.ssh/authorized_keys && \
    mkdir -p /etc/supervisor/conf.d && \
    cp /var/lib/dist/tests/conf/supervisord.conf /etc/supervisor/supervisord.conf && \
    cp /var/lib/dist/docker/base/ssh.conf /etc/supervisor/conf.d/ssh.conf
CMD ["/usr/local/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]
