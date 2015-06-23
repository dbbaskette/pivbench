drop table if exists catalog_returns;

create table catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           bigint               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          float                  ,
    cr_return_tax             float                  ,
    cr_return_amt_inc_tax     float                  ,
    cr_fee                    float                  ,
    cr_return_ship_cost       float                  ,
    cr_refunded_cash          float                 ,
    cr_reversed_charge        float                  ,
    cr_store_credit           float                  ,
    cr_net_loss               float
)
with (appendonly=true, ORIENTATION=$ORIENTATION)
distributed by (cr_item_sk, cr_order_number)
partition by range(cr_returned_date_sk)

(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);