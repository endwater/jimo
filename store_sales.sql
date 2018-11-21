create database if not exists db;
use db;

create data schema if not exists store_sales_schema
TYPE CSV
FIELDS(
        ss_sold_date_sk          int64,
        ss_sold_time_sk          int64,
    ss_item_sk               int64,
    ss_customer_sk           int64,
    ss_cdemo_sk              int64,
    ss_hdemo_sk              int64,
    ss_addr_sk               int64,
    ss_store_sk              int64,
    ss_promo_sk              int64,
    ss_ticket_number         int64,
    ss_quantity              int64,
    ss_wholesale_cost        number(10,2),
    ss_list_price            number(10,2),
    ss_sales_price           number(10,2),
    ss_ext_discount_amt      number(10,2),
    ss_ext_sales_price       number(10,2),
    ss_ext_wholesale_cost    number(10,2),
    ss_ext_list_price        number(10,2),
    ss_ext_tax               number(10,2),
    ss_coupon_amt            number(10,2),
    ss_net_paid              number(10,2),
    ss_net_paid_inc_tax      number(10,2),
    ss_net_profit            number(10,2),
    d_empty                  string,
    d_date                   datetime format "%Y-%m-%d %H:%M:%S" value valuemap(db.date_dim.date_dim_map_d_date,ss_sold_date_sk)
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";


create dataset file store_sales_dataset
(
    schema:store_sales_schema,
    files:(
            (filename:"/tpcds/data100G/store_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create hyper table if not exists store_sales using store_sales_schema;


create job store_sales_job(1)
begin
create dataproc load_data store_sales_doc
(
    input:store_sales_dataset,
    table:store_sales
);
end;
run job store_sales_job(threadnum:6,processnum:3);
