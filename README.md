Module: Synchronize logical slots
=========================================

This module provides a mechanism to synchronize the logical replication slots to synchronous streaming replication standbys

# Prerquisite

1. EDB Advanced Server 12.x installation using RPMs. To install EDB Advanced Server 12.x using RPMs, refer EDB Documents
2. gcc
3. make
4. openssl-devel
5. location of pg_config of EDB Advanced Server should be in PATH environment variable
6. Set max_worker_processes parameter to allow background worker in EPAS.
7. Make sure you have pg_hba entry for standbys on primary for replication user to connect to all databases.
8. Make sure replication user has `pg_read_all_stats` privileges.

# Installation

Following are the steps for installing this module:

1. compile the module using following command on EPAS primary and synchronous standby
```
export PATH=/usr/edb/as12/bin:$PATH
make 
make install
```
In case you don't have LLVM libraries on the system, you can use following commands for installing this module:
```
export PATH=/usr/edb/as12/bin:$PATH
with_llvm=false make -e
with_llvm=false make install -e
```

2. Connect to the `postgres` and databases which has logical replication slot and execute following command to enable to module
```
psql -U enterprisedb -c "CREATE EXTENSION synchronize_logical_slots CASCADE;" -d postgres
```
3. Update the following parameters in postgresql.conf for master and synchronous standbys:
```
shared_preload_libraries = '$libdir/synchronize_logical_slots_launcher' # sync_logical_slot library for background worker
```
4. Restart the master and synchronous standby service
```
systemctl edb-as-12 stop
systemctl edb-as-12 start
```

# Example

```
gcc -std=gnu99 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/edb-as12-icu53.1/include/ -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fPIC -I. -I./ -I/usr/edb/as12/include/server -I/usr/edb/as12/include/internal -I/usr/libexec/edb-as12-icu53.1/include/ -I/usr/include/et -D_GNU_SOURCE -I/usr/include/libxml2  -I/usr/include  -c -o synchronize_logical_slots.o synchronize_logical_slots.c
gcc -std=gnu99 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/edb-as12-icu53.1/include/ -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fPIC synchronize_logical_slots.o -L/usr/edb/as12/lib -L/usr/libexec/edb-as12-icu53.1/lib/  -L/usr/lib64/llvm5.0/lib  -L/usr/lib64 -Wl,--as-needed -Wl,-rpath,'/usr/edb/as12/lib',--enable-new-dtags  -shared -o synchronize_logical_slots.so
gcc -std=gnu99 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/edb-as12-icu53.1/include/ -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fPIC -I. -I./ -I/usr/edb/as12/include/server -I/usr/edb/as12/include/internal -I/usr/libexec/edb-as12-icu53.1/include/ -I/usr/include/et -D_GNU_SOURCE -I/usr/include/libxml2  -I/usr/include  -c -o synchronize_logical_slots_launcher.o synchronize_logical_slots_launcher.c
gcc -std=gnu99 -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/edb-as12-icu53.1/include/ -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fPIC synchronize_logical_slots_launcher.o -L/usr/edb/as12/lib -L/usr/libexec/edb-as12-icu53.1/lib/  -L/usr/lib64/llvm5.0/lib  -L/usr/lib64 -Wl,--as-needed -Wl,-rpath,'/usr/edb/as12/lib',--enable-new-dtags  -shared -o synchronize_logical_slots_launcher.so
/bin/mkdir -p '/usr/edb/as12/share/extension'
/bin/mkdir -p '/usr/edb/as12/share/extension'
/bin/mkdir -p '/usr/edb/as12/lib'
/bin/install -c -m 644 .//synchronize_logical_slots.control '/usr/edb/as12/share/extension/'
/bin/install -c -m 644 .//synchronize_logical_slots--1.0.sql  '/usr/edb/as12/share/extension/'
/bin/install -c -m 755  synchronize_logical_slots.so synchronize_logical_slots_launcher.so '/usr/edb/as12/lib/'

psql -p 5432 -c "CREATE EXTENSION synchronize_logical_slots CASCADE;" -d postgres
NOTICE:  installing required extension "dblink"
CREATE EXTENSION

psql -p 5432 -c "CREATE EXTENSION synchronize_logical_slots CASCADE;" -d edb
NOTICE:  installing required extension "dblink"
CREATE EXTENSION

echo "shared_preload_libraries = '$libdir/synchronize_logical_slots_launcher'" >>$PGDATA/postgresql.conf
 
 systemctl edb-as-12 stop
 systemctl edb-as-12 start
 ```
And enable the following parameter on master and standbys:
```
echo "shared_preload_libraries = '$libdir/synchronize_logical_slots_launcher'" >>$PGDATA/postgresql.conf
 ```
# Limitation
* Currently this module works for synchronizing the logical replication slots for **SYNCHRONOUS STANDBY**. 
* If one of the standby gets promoted, then please make sure to remove the unused logical replication slots manually.
