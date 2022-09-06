# pg_anon Dockerfile

## Usage

Make image:

```bash
cd pg_anon/docker
make PG_VERSION=13
```

Push image:

```bash
docker tag $(docker images -q | head -n 1) pg_anon:pg13

docker save -o pg_anon.tar pg_anon

curl --fail -v --user 'user:password' --upload-file pg_anon.tar https://nexus.tantorlabs.ru/repository/tantorlabs-raw/
```

## Run container

```bash
# docker rm -f pg_anon

docker run --name pg_anon -d pg_anon:pg13
docker exec -it pg_anon bash
python3 test/full_test.py -v
exit

```