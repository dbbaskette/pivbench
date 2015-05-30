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
    2015-05-30 10:33:15,539 INFO Executing HAWQ Queries
    2015-05-30 10:33:15,539 INFO ---------------------------------
    2015-05-30 10:33:15,540 INFO Running all Queries
    2015-05-30 10:40:19,821 INFO Query: query_55   Execution Time(s): xxx.xx  Rows Returned: 100
    2015-05-30 10:45:00,443 INFO Query: query_30   Execution Time(s): xxx.xx  Rows Returned: 100
    2015-05-30 11:14:15,389 INFO Query: query_79   Execution Time(s): xxx.xx  Rows Returned: 100
    2015-05-30 11:18:12,482 INFO Query: query_92   Execution Time(s): xxx.xx  Rows Returned: 1
    2015-05-30 11:25:39,422 INFO Query: query_85   Execution Time(s): xxx.xx  Rows Returned: 71
