EXTENSION = livewire
DATA = livewire--1.sql

release: 
	cat sql/*.sql > livewire--1.sql
test:
	pg_prove --verbose --pset tuples_only=1  $(TESTS)

containertest:
	podman run --rm --name livewiretest --detach \
		--env POSTGRES_HOST_AUTH_METHOD=trust \
		--volume ./:/livewire --workdir /livewire \
		quay.io/rhysallister/elephant
	sleep 7
	podman exec livewiretest make
	podman exec livewiretest make install
	podman exec livewiretest make installcheck PGUSER=postgres

cleanup:
	rm -f -r tests/results
	rm -f tests/regression.diffs
	rm -f tests/regression.out
	rm -f livewire--*.sql
	@podman stop --ignore livewiretest
	dropdb --if-exists $(CONTRIB_TESTDB) 
	
clean: cleanup

TESTS = $(wildcard tests/sql/*.sql)

REGRESS = $(patsubst tests/sql/%.sql,%,$(TESTS))

REGRESS_OPTS = --inputdir=tests \
	--outputdir=tests \
	--load-extension=pgtap \
	--load-language plpgsql \
	--load-extension=postgis \
	--load-extension=pgrouting \
	--load-extension=livewire	

# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
