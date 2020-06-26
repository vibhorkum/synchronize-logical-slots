MODULES = synchronize_logical_slots synchronize_logical_slots_launcher
PGFILEDESC = "synchronize_logical_slots - utility for synchronizing logical slots on standby"
EXTENSION = synchronize_logical_slots
DATA = synchrnoize_logical_slots--1.0.sql
PG_CONFIG = pg_config
EXTRA_CLEAN = synchronize_logical_slots.so synchronize_logical_slots.o synchronize_logical_slots_launcher.so synchronize_logical_slots_launcher.o
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
