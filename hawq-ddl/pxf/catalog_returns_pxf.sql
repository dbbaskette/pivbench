DROP EXTERNAL TABLE IF EXISTS catalog_returns_pxf;
CREATE EXTERNAL TABLE catalog_returns_pxf
(
 like catalog_returns
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/catalog_returns?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
