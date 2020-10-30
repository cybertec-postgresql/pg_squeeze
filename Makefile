PG_CONFIG ?= pg_config
MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o worker.o pgstatapprox.o $(WIN32RES)
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.2.sql
DATA_built = pg_squeeze--1.2--1.3.sql

REGRESS = squeeze

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# TODO Remove this as soon as PG 10 is EOL. The problem is that it wasn't
# noticed soon enough that PG 10 does not support array of domain type.
ifeq ($(MAJORVERSION),10)
pg_squeeze--1.2--1.3.sql:	pg_squeeze--1.2--1.3.sql.in.pg_10
	echo $<
	cp $< $@
endif
