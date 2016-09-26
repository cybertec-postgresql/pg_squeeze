MODULE_big = pg_squeeze
OBJS = pg_squeeze.o concurrent.o decode_plugin.o worker.o $(WIN32RES)
PGFILEDESC = "pg_squeeze - a tool to remove unused space from a relation."

EXTENSION = pg_squeeze
DATA = pg_squeeze--1.0.sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
