FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Moskow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN sed -i "s/archive.ubuntu.com/mirror.yandex.ru/g" /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    lsb-release libssl-dev gnupg openssl libssl-dev\
    debhelper debootstrap devscripts make equivs \
    python3=3.6.7-1~18.04

RUN alias python3.6=python3

WORKDIR /root/pgconsul
COPY . /root/pgconsul

RUN mk-build-deps --build-dep --install --tool='apt-get -o Debug::pkgProblemResolver=yes --no-install-recommends --yes' debian/control
RUN dpkg-buildpackage -us -uc
