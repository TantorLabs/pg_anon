FROM ubuntu:20.04

RUN apt update && \
  apt install -y wget vim nano htop tree sysbench net-tools sysstat less iotop && \
  apt -y install curl gpg gnupg2 apt-transport-https lsb-release ca-certificates && \
  apt -y install software-properties-common && \
  apt -y install python3-pip && \
  apt install -y locales && locale-gen en_US.UTF-8 && \
  rm -rf /tmp/* && apt purge -y --auto-remove && apt clean -y autoclean

ARG PG_VERSION
ARG DEBIAN_FRONTEND=noninteractive

# RUN if [ "$PG_VERSION" = "13" ]; then curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc| gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg; fi
# RUN if [ "$PG_VERSION" = "13" ]; then echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list; fi
# RUN if [ "$PG_VERSION" = "13" ]; then apt update ; fi


# =======================================
# Ubuntu 20.04
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc| gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list
RUN apt update
# =======================================

RUN apt -y install postgresql-${PG_VERSION} postgresql-client-${PG_VERSION}

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
