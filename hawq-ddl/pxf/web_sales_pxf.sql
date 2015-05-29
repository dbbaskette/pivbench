DROP EXTERNAL TABLE IF EXISTS web_sales_pxf;
CREATE EXTERNAL TABLE web_sales_pxf
(
like web_sales_nopart
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/web_sales?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
