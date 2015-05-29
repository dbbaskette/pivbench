DROP EXTERNAL TABLE IF EXISTS warehouse_pxf;
CREATE EXTERNAL TABLE warehouse_pxf
(
   like warehouse
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/warehouse?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;