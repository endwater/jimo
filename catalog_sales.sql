create database if not exists db;
use db;


create data schema if not exists catalog_sales_schema
TYPE CSV
FIELDS(
        cs_sold_date_sk          int64,
        cs_sold_time_sk          int64,
        cs_ship_date_sk          int64,
        cs_bill_customer_sk      int64,
        cs_bill_cdemo_sk         int64,
        cs_bill_hdemo_sk         int64,
        cs_bill_addr_sk          int64,
        cs_ship_customer_sk      int64,
        cs_ship_cdemo_sk         int64,
        cs_ship_hdemo_sk         int64,
        cs_ship_addr_sk          int64,
        cs_call_center_sk        int64,
        cs_catalog_page_sk       int64,
        cs_ship_mode_sk          int64,
        cs_warehouse_sk          int64,
        cs_item_sk               int64,
        cs_promo_sk              int64,
        cs_order_number          int64,
        cs_quantity              int64,
        cs_wholesale_cost        number(10,2),
        cs_list_price            number(10,2),
        cs_sales_price           number(10,2),
        cs_ext_discount_amt      number(10,2),
        cs_ext_sales_price       number(10,2),
        cs_ext_wholesale_cost    number(10,2),
        cs_ext_list_price        number(10,2),
        cs_ext_tax               number(10,2),
        cs_coupon_amt            number(10,2),
        cs_ext_ship_cost         number(10,2),
        cs_net_paid              number(10,2),
        cs_net_paid_inc_tax      number(10,2),
        cs_net_paid_inc_ship     number(10,2),
        cs_net_paid_inc_ship_tax number(10,2),
        cs_net_profit            number(10,2),
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file catalog_sales_dataset
(
    schema:catalog_sales_schema,
    files:(
            (filename:"/tpcds/data100G/catalog_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create hyper table if not exists catalog_sales using catalog_sales_schema;


create job catalog_sales_job(1)
begin
create dataproc load_data catalog_sales_doc
(
    input:catalog_sales_dataset,
    table:catalog_sales
);
end;
run job catalog_sales_job(threadnum:6,processnum:3);
