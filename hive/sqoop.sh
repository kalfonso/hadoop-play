#!/bin/sh

sqoop import \
--connect jdbc:mysql://ip-172-31-5-92.ap-southeast-2.compute.internal/employees \
--username hive \
--password password \
--table $1 \
--hcatalog-database employees \
--hcatalog-table $1_temp \
--create-hcatalog-table \
--hcatalog-storage-stanza "stored as orcfile" \
-m 1 \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--driver com.mysql.jdbc.Driver
