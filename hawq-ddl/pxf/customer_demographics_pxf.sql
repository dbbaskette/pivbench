DROP EXTERNAL TABLE IF EXISTS customer_demographics_pxf;
CREATE EXTERNAL TABLE customer_demographics_pxf
(
  like customer_demographics
)
LOCATION ('pxf://$NAMENODE:50070/$BASE/$SIZE/customer_demographics?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;