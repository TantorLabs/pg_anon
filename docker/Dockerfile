FROM ubuntu:22.04

ARG PG_VERSION
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && \
  apt install -y wget vim nano htop tree sysbench net-tools sysstat iotop && \
  apt -y install postgresql-${PG_VERSION} postgresql-client-${PG_VERSION} && \
  apt -y install software-properties-common && \
  apt -y install python3-pip && \
  apt install -y locales && locale-gen en_US.UTF-8 && \
  rm -rf /tmp/* && apt purge -y --auto-remove && apt clean -y autoclean

# Add pg_anon
ADD ./pg_anon /usr/share/pg_anon

RUN pip3 install -r /usr/share/pg_anon/requirements.txt

EXPOSE 5432

ENV PG_VERSION=${PG_VERSION}

ADD entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ADD entrypoint_dbg.sh /entrypoint_dbg.sh
RUN chmod +x /entrypoint_dbg.sh

ADD motd /etc/motd

WORKDIR /usr/share/pg_anon

ENTRYPOINT exec /entrypoint.sh
# ENTRYPOINT exec /entrypoint_dbg.sh
