DROP EXTERNAL TABLE IF EXISTS web_page_pxf;
CREATE EXTERNAL TABLE web_page_pxf
(
   like web_page
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/web_page?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;