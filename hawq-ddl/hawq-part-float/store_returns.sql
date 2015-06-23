drop table if exists store_returns;

create table store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          bigint               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             float                  ,
    sr_return_tax             float                  ,
    sr_return_amt_inc_tax     float                  ,
    sr_fee                    float                  ,
    sr_return_ship_cost       float                  ,
    sr_refunded_cash          float                  ,
    sr_reversed_charge        float                  ,
    sr_store_credit           float                  ,
    sr_net_loss               float
)
with (appendonly=true, ORIENTATION=$ORIENTATION)
distributed by (sr_item_sk, sr_ticket_number)
partition by range(sr_returned_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);