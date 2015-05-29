DROP EXTERNAL TABLE IF EXISTS promotion_pxf;
CREATE EXTERNAL TABLE promotion_pxf
(
 like promotion
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SIZE/promotion?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;