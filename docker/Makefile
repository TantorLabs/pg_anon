.DEFAULT_GOAL := build
.PHONY: build

check-env:
ifndef PG_VERSION
	$(error PG_VERSION is undefined)
endif

default: build

build:
	if test -d pg_anon; \
	then cd pg_anon && git pull; \
	else git clone https://github.com/TantorLabs/pg_anon.git; \
	fi

	docker build -t pg_anon:pg${PG_VERSION} --build-arg PG_VERSION=${PG_VERSION} .

.PHONY: clean
clean:
	rm -rf pg_anon

.PHONY: prune
prune:
	docker images prune -a
	docker system prune -a -f
