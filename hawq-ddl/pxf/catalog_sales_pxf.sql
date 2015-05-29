DROP EXTERNAL TABLE IF EXISTS catalog_sales_pxf;
CREATE EXTERNAL TABLE catalog_sales_pxf
(
  like catalog_sales
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/catalog_sales?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
