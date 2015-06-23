drop table if exists web_sales;
create table web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           bigint               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         float                  ,
    ws_list_price             float                  ,
    ws_sales_price            float                  ,
    ws_ext_discount_amt       float                  ,
    ws_ext_sales_price        float                  ,
    ws_ext_wholesale_cost     float                  ,
    ws_ext_list_price         float                  ,
    ws_ext_tax                float                  ,
    ws_coupon_amt             float                  ,
    ws_ext_ship_cost          float                  ,
    ws_net_paid               float                  ,
    ws_net_paid_inc_tax       float                  ,
    ws_net_paid_inc_ship      float                  ,
    ws_net_paid_inc_ship_tax  float                  ,
    ws_net_profit             float
)
with (appendonly=true, ORIENTATION=$ORIENTATION)
distributed by (ws_item_sk, ws_order_number)
partition by range(ws_sold_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);