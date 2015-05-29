


DROP EXTERNAL TABLE IF EXISTS time_dim_pxf;
CREATE EXTERNAL TABLE time_dim_pxf
(
  like time_dim
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/time_dim?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;