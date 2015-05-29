DROP EXTERNAL TABLE IF EXISTS store_returns_pxf;
CREATE EXTERNAL TABLE store_returns_pxf
(
  like store_returns
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/store_returns?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;