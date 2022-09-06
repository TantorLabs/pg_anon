# pg_anon Dockerfile

## Usage

Make image:

```bash
cd pg_anon/docker
make PG_VERSION=13
```

Push image:

```bash
docker tag $(docker images -q | head -n 1) USER/pg_anon:dbc_pg13 && \
docker push USER/pg_anon:dbc_pg13
```

## Run container

```bash

```