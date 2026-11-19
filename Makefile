PG_CONFIG ?= pg_config
MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o worker.o pgstatapprox.o $(WIN32RES)
EXTRA_CLEAN = pgstatapprox.c
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.2.sql pg_squeeze--1.2--1.3.sql pg_squeeze--1.3--1.4.sql \
pg_squeeze--1.4--1.5.sql pg_squeeze--1.5--1.6.sql pg_squeeze--1.6--1.7.sql \
pg_squeeze--1.7--1.8.sql pg_squeeze--1.8--1.9.sql
DOCS = pg_squeeze.md

REGRESS = squeeze

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# TODO Use intcmp when all OSes seem to support the corresponding version of
# make.
#OLD := $(intcmp $(MAJORVERSION),19,y)
OLD := $(shell test $(MAJORVERSION) -lt 19 && echo y)

ifeq ($(OLD),y)
pgstatapprox.c:	pgstatapprox.in.old.c
	cp $< $@
else
pgstatapprox.c:	pgstatapprox.in.c
	cp $< $@
endif
