DROP EXTERNAL TABLE IF EXISTS web_returns_pxf;
CREATE EXTERNAL TABLE web_returns_pxf
(
  like web_returns_nopart
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/web_returns?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;
