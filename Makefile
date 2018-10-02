EXTENSION = livewire          # the extension name
DATA = livewire--0.2.0.sql

release: 
	cat sql/*.sql > livewire--0.2.0.sql


test:
	pg_prove --verbose --pset tuples_only=1 $(TESTS)

cleantest:
	rm -f -r tests/results
	rm -f tests/regression.diffs
	rm -f tests/regression.out

TESTS = $(wildcard tests/sql/*.sql)

REGRESS = $(patsubst tests/sql/%.sql,%,$(TESTS))

REGRESS_OPTS = --inputdir=tests --outputdir=tests --load-language plpgsql \
	--load-extension=postgis --load-extension=pgrouting \
	--load-extension=livewire	

# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
