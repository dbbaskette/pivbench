DROP EXTERNAL TABLE IF EXISTS store_pxf;
CREATE EXTERNAL TABLE store_pxf
(
   like store
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/store?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;