drop table if exists web_returns;
create table web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           bigint               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             float                  ,
    wr_return_tax             float                  ,
    wr_return_amt_inc_tax     float                  ,
    wr_fee                    float                  ,
    wr_return_ship_cost       float                  ,
    wr_refunded_cash          float                  ,
    wr_reversed_charge        float                  ,
    wr_account_credit         float                  ,
    wr_net_loss               float
)
with (appendonly=true, ORIENTATION=$ORIENTATION)
distributed by (wr_item_sk, wr_order_number)
partition by range(wr_returned_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);