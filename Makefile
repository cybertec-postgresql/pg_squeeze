PG_CONFIG ?= pg_config
MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o worker.o pgstatapprox.o $(WIN32RES)
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.1.sql pg_squeeze--1.0--1.1.sql

REGRESS = squeeze

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
