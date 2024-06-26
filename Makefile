PG_CONFIG ?= pg_config
MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o worker.o pgstatapprox.o $(WIN32RES)
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.2.sql pg_squeeze--1.2--1.3.sql pg_squeeze--1.3--1.4.sql \
pg_squeeze--1.4--1.5.sql pg_squeeze--1.5--1.6.sql pg_squeeze--1.6--1.7.sql
DOCS = pg_squeeze.md

REGRESS = squeeze

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

