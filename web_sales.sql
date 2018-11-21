create database if not exists db;
use db;

create data schema if not exists web_sales_schema
TYPE CSV
FIELDS(
        ws_sold_date_sk          int64,
        ws_sold_time_sk          int64,
        ws_ship_date_sk          int64,
        ws_item_sk               int64,
        ws_bill_customer_sk      int64,
        ws_bill_cdemo_sk         int64,
        ws_bill_hdemo_sk         int64,
        ws_bill_addr_sk          int64,
        ws_ship_customer_sk      int64,
        ws_ship_cdemo_sk         int64,
        ws_ship_hdemo_sk         int64,
        ws_ship_addr_sk          int64,
        ws_web_page_sk           int64,
        ws_web_site_sk           int64,
        ws_ship_mode_sk          int64,
        ws_warehouse_sk          int64,
        ws_promo_sk              int64,
        ws_order_number          int64,
        ws_quantity              int64,
        ws_wholesale_cost        number(10,2),
        ws_list_price            number(10,2),
        ws_sales_price           number(10,2),
        ws_ext_discount_amt      number(10,2),
        ws_ext_sales_price       number(10,2),
        ws_ext_wholesale_cost    number(10,2),
        ws_ext_list_price        number(10,2),
        ws_ext_tax               number(10,2),
        ws_coupon_amt            number(10,2),
        ws_ext_ship_cost         number(10,2),
        ws_net_paid              number(10,2),
        ws_net_paid_inc_tax      number(10,2),
        ws_net_paid_inc_ship     number(10,2),
        ws_net_paid_inc_ship_tax number(10,2),
        ws_net_profit            number(10,2),
        d_empty                  string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file web_sales_dataset
(
    schema:web_sales_schema,
    files:(
            (filename:"/tpcds/data100G/web_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create hyper table if not exists web_sales using web_sales_schema;


create job web_sales_job(1)
begin
create dataproc load_data web_sales_doc
(
    input:web_sales_dataset,
    table:web_sales
);
end;
run job web_sales_job(threadnum:6,processnum:3);
