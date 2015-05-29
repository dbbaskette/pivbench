# pivbench
###Pivotal HAWQ TPC-DS Benchmarking Tool
--

Automation of the TPC-DS benchmark for Pivotal HAWQ.
The application is broken into functions:

* **gen (Generate)** - This takes the size and HDFS target of the benchmark data and leverages MapReduce to create the datafiles.   It's a patched version of the generator included with Hortonworks Hive-Bench.  It also changes the Replication factor to 2 for the directory to save capacity.  
* **load** - This module creates the HAWQ tables and PXF External tables and moves the Raw - Pipe Delimited data into HAWQ.  For the larger fact tables, non-partitioned tables (labeled as _nopart) as created to speed the data load.
* **part (Partition)** - Takes number of partitions as a parameter and creates Partitioned versions of the larger fact tables with the _nopart name removed.  It then does a insert into X select * from Y to load the tables.  This method allows for easier modification in the event new partition sizes are needed.
* **analyze** - This module will analyze all the fact and diminsion tables.
*  **query** - Runs any single query, or all 99 TPC-DS queries against the database and records the elapsed times of each.  It also clears the buffers and cache on all cluster nodes before every query executes.