DROP EXTERNAL TABLE IF EXISTS household_demographics_pxf;
CREATE EXTERNAL TABLE household_demographics_pxf
(
    like household_demographics
) LOCATION ('pxf://$NAMENODE:50070/$BASE/$SCALE/household_demographics?profile=HdfsTextSimple')
FORMAT 'TEXT' (DELIMITER '|' NULL E'' FILL MISSING FIELDS) ENCODING 'latin1'
SEGMENT REJECT LIMIT 1 PERCENT;