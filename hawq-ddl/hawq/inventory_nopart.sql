drop table if exists inventory_nopart;
create table inventory_nopart
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer
)
WITH (APPENDONLY=TRUE, COMPRESSTYPE=QUICKLZ)
distributed by (inv_date_sk, inv_item_sk, inv_warehouse_sk);