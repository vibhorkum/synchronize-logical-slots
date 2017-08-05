Module: Sync logical Slot
=========================================

This module provides a mechanism to synchronize the logical replication slots, using test_decoding, to synchronous streaming replication standbys

# Prerquisit

1. EDB Advanced Server 9.6 installation using RPMs. To install EDB Advanced Server 9.6 using RPMs, refer EDB Documents
2. gcc
3. make
4. location of pg_config of EPAS should be in PATH environment variable
5. Set max_worker_processes parameter to allow background worker in EPAS

# Installation

Following are the steps for installing this module:

1. compile the module using following command on EPAS primary and synchronous standby
```
export PATH=/usr/edb/as9.6/bin:$PATH
make 
make install
```
2. Connect to the database which has logical replication slot and execute following command to enable to module
```
CREATE EXTENSION slot_timelines CASCADE;
```
3. Using function following function create FDW server for master:
```
SELECT failover_logical_slot_init('${MASTER_IP}','${PGPORT}',current_database,'superuser','super_user_password');
```
Example:
```
SELECT failover_logical_slot_init('${MASTER_IP}','5432','edb','enterprisedb','edb');
```
3. Update the following parameters in postgresql.conf for master and synchronous standbys:
```
sync_logical_slot.database = 'edb'  # name of database which has logical replication slot
shared_preload_libraries = 'sync_logical_slot' # sync_logical_slot library for background worker
```
4. Restart the master and synchronous standby service
```
systemctl edb-as-9.6 stop
systemctl edb-as-9.6 start
```

# Example

```
make install
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/icu-edb53.1/include -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fpic -I. -I./ -I/usr/edb/as9.6/include/server -I/usr/edb/as9.6/include/internal -I/usr/include/et -D_GNU_SOURCE -I/usr/include/libxml2 -I/usr/libexec/icu-edb53.1/include   -I/usr/include  -c -o sync_logical_slot.o sync_logical_slot.c
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/icu-edb53.1/include -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fpic -L/usr/edb/as9.6/lib -Wl,-rpath,/usr/libexec/icu-edb53.1/lib -L/usr/libexec/icu-edb53.1/lib   -L/usr/lib64 -L/usr/libexec/icu-edb53.1/lib  -Wl,--as-needed -Wl,-rpath,'/usr/edb/as9.6/lib',--enable-new-dtags  -shared -o sync_logical_slot.so sync_logical_slot.o
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/icu-edb53.1/include -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fpic -I. -I./ -I/usr/edb/as9.6/include/server -I/usr/edb/as9.6/include/internal -I/usr/include/et -D_GNU_SOURCE -I/usr/include/libxml2 -I/usr/libexec/icu-edb53.1/include   -I/usr/include  -c -o slot_timelines.o slot_timelines.c
gcc -Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -I/usr/libexec/icu-edb53.1/include -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong --param=ssp-buffer-size=4 -grecord-gcc-switches -m64 -mtune=generic -I/usr/include/et -fpic -L/usr/edb/as9.6/lib -Wl,-rpath,/usr/libexec/icu-edb53.1/lib -L/usr/libexec/icu-edb53.1/lib   -L/usr/lib64 -L/usr/libexec/icu-edb53.1/lib  -Wl,--as-needed -Wl,-rpath,'/usr/edb/as9.6/lib',--enable-new-dtags  -shared -o slot_timelines.so slot_timelines.o
/usr/bin/mkdir -p '/usr/edb/as9.6/share/extension'
/usr/bin/mkdir -p '/usr/edb/as9.6/share/extension'
/usr/bin/mkdir -p '/usr/edb/as9.6/lib'
/usr/bin/install -c -m 644 .//slot_timelines.control '/usr/edb/as9.6/share/extension/'
/usr/bin/install -c -m 644 .//slot_timelines--1.0.sql  '/usr/edb/as9.6/share/extension/'
/usr/bin/install -c -m 755  sync_logical_slot.so slot_timelines.so '/usr/edb/as9.6/lib/'

psql -p 5432 -c "CREATE EXTENSION slot_timelines CASCADE;"
NOTICE:  installing required extension "postgres_fdw"
NOTICE:  installing required extension "dblink"
NOTICE:  installing required extension "adminpack"
CREATE EXTENSION

psql -p 5432 -c "SELECT failover_logical_slot_init('${MASTER_IP}','5432','edb','enterprisedb','edb');"
NOTICE:  server "master_fdw" does not exist, skipping
               failover_logical_slot_init               
--------------------------------------------------------
 update following parameters in postgresql.conf
 sync_logical_slot.database = 'edb'
 sync_logical_slot.master_fdw = 'master_fdw'
 shared_preload_libraries = '$libdir/sync_logical_slot'
(4 rows)


echo "sync_logical_slot.database = 'edb'" >> $PGDATA/postgresql.conf
echo " sync_logical_slot.database = 'edb'
 sync_logical_slot.master_fdw = 'master_fdw'
 shared_preload_libraries = '$libdir/sync_logical_slot'" >>$PGDATA/postgresql.conf
 
 systemctl edb-as-9.6 stop
 systemctl edb-as-9.6 start
 ```

Build a synchronous streaming replication standby. Please refer following link:
http://vibhork.blogspot.in/2011/10/asynchronoussynchronous-streaming.html

And enable the following parameter as on master for standby:
```
echo "sync_logical_slot.database = 'edb'" >> $PGDATA/postgresql.conf
echo " sync_logical_slot.database = 'edb'
 sync_logical_slot.master_fdw = 'master_fdw'
 shared_preload_libraries = '$libdir/sync_logical_slot'" >>$PGDATA/postgresql.conf
 systemctl edb-as-9.6 stop
 systemctl edb-as-9.6 start
 ```
# Limitation
Currently this module works for synchronoizing the logical slots for one database. 
In future we will remove this limitaiton.
