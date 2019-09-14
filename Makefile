EXTENSION = livewire          # the extension name
DATA = livewire--0.4.0.sql

release: 
	cat sql/*.sql > livewire--0.4.0.sql
test:
	pg_prove --verbose --pset tuples_only=1  $(TESTS)

cleanup:
	rm -f -r tests/results
	rm -f tests/regression.diffs
	rm -f tests/regression.out
	dropdb --if-exists $(CONTRIB_TESTDB)

clean: cleanup

TESTS = $(wildcard tests/sql/*.sql)

REGRESS = $(patsubst tests/sql/%.sql,%,$(TESTS))

REGRESS_OPTS = --inputdir=tests \
	--outputdir=tests \
	--load-language plpgsql \
	--load-extension=postgis \
	--load-extension=pgrouting \
	--load-extension=livewire	

# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
