# pg_anon Dockerfile

## Usage

Make image:

```bash
cd pg_anon/docker
make PG_VERSION=15
docker tag $(docker images -q | head -n 1) pg_anon:pg15
```

Push image:

```bash
docker tag $(docker images -q | head -n 1) pg_anon:pg15

docker save -o pg_anon_22_10_23.tar pg_anon:pg15

curl --fail -v --user 'user:password' --upload-file pg_anon_22_10_23.tar https://nexus.tantorlabs.ru/repository/tantorlabs-raw/
```

## Run container

```bash
# If "The container name "/pg_anon" is already in use"
# docker rm -f pg_anon

docker run --name pg_anon -d pg_anon:pg15
docker exec -it pg_anon bash
chown -R postgres .
su - postgres
python3 test/full_test.py -v
exit 

# Run and mount directory from HOST to /usr/share/pg_anon_from_host
docker rm -f pg_anon
docker run --name pg_anon -v $PWD:/usr/share/pg_anon -d pg_anon:pg15
```

If tests raised error like: `asyncpg.exceptions.ExternalRoutineError: program "gzip > ... *.dat.gz" failed`

See: [Configure permission](https://github.com/TantorLabs/pg_anon#configure-permission)

## Load saved image

```bash
docker load < pg_anon_22_9_12.tar
```

## How to debug container

```bash
docker exec -it pg_anon bash
>>
	Error response from daemon: Container c876d... is not running

docker logs c876d...

# Fix errors in entrypoint.sh
# Set "ENTRYPOINT exec /entrypoint_dbg.sh" in Dockerfile

docker rm -f pg_anon
make PG_VERSION=15
docker tag $(docker images -q | head -n 1) pg_anon:pg15
docker run --name pg_anon -d pg_anon:pg15
docker exec -it pg_anon bash
```
