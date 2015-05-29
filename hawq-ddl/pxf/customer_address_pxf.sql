
DROP EXTERNAL TABLE IF EXISTS customer_address_pxf;
CREATE EXTERNAL TABLE customer_address_pxf
(
  like customer_address
)
LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/customer_address?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;