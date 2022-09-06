.DEFAULT_GOAL := build
.PHONY: build

check-env:
ifndef PG_VERSION
	$(error PG_VERSION is undefined)
endif

default: build

build:
	if test -d pg_anon; \
	then echo "pg_anon exists"; \
	else git clone https://bitbucket.org/awide/pg_anon.git; \
	fi

	docker build -t pg_anon:pg${PG_VERSION} --build-arg PG_VERSION=${PG_VERSION} .

.PHONY: clean
clean:
	rm -rf pg_anon

.PHONY: prune
prune:
	docker images prune -a
	docker system prune -a -f