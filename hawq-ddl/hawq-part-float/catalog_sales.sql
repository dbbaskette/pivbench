drop table if exists catalog_sales;

create table catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               ,
    cs_promo_sk               integer                       ,
    cs_order_number           bigint               ,
    cs_quantity               integer                       ,
    cs_wholesale_cost         float                  ,
    cs_list_price             float                  ,
    cs_sales_price            float                  ,
    cs_ext_discount_amt       float                  ,
    cs_ext_sales_price        float                  ,
    cs_ext_wholesale_cost     float                  ,
    cs_ext_list_price         float                  ,
    cs_ext_tax                float                  ,
    cs_coupon_amt             float                  ,
    cs_ext_ship_cost          float                  ,
    cs_net_paid               float                  ,
    cs_net_paid_inc_tax       float                  ,
    cs_net_paid_inc_ship      float                  ,
    cs_net_paid_inc_ship_tax  float                  ,
    cs_net_profit             float
)
with (appendonly=true, ORIENTATION=$ORIENTATION)
distributed by (cs_item_sk, cs_order_number)
partition by range(cs_sold_date_sk)
(start(2450815) INCLUSIVE end(2453005) INCLUSIVE every ($PARTS),
default partition outliers
);
