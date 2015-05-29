DROP EXTERNAL TABLE IF EXISTS date_dim_pxf;
CREATE EXTERNAL TABLE date_dim_pxf
(
  like date_dim
)
LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/date_dim?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
