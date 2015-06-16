drop table if exists inventory;
create table inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer
)
with (appendonly=true, ORIENTATION=$ORIENTATION,ROWGROUPSIZE=1073741823)
distributed by (inv_date_sk, inv_item_sk, inv_warehouse_sk)
partition by range(inv_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);