insert into $PARTNAME select * from inventory_nopart where inv_date_sk between $PARTVALUE1 and $PARTVALUE2;