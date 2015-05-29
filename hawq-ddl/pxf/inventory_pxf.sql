DROP EXTERNAL TABLE IF EXISTS inventory_pxf;
CREATE EXTERNAL TABLE inventory_pxf

(
  like inventory
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/inventory?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
