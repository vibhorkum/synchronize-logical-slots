MODULES = sync_logical_slot slot_timelines
PGFILEDESC = "slot_timelines - utility for slot timeline following"
EXTENSION = slot_timelines
DATA = slot_timelines--1.0.sql
PG_CONFIG = pg_config
EXTRA_CLEAN = slot_timelines.so sync_logical_slot.so sync_logical_slot.o slot_timelines.o
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
