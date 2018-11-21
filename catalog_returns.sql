create database if not exists db;
use db;

create data schema if not exists catalog_returns_schema
TYPE CSV
FIELDS(
    cr_returned_date_sk      int64,
    cr_returned_time_sk      int64,
    cr_item_sk               int64,
    cr_refunded_customer_sk  int64,
    cr_refunded_cdemo_sk     int64,
    cr_refunded_hdemo_sk     int64,
    cr_refunded_addr_sk      int64,
    cr_returning_customer_sk int64,
    cr_returning_cdemo_sk    int64,
    cr_returning_hdemo_sk    int64,
    cr_returning_addr_sk     int64,
    cr_call_center_sk        int64,
    cr_catalog_page_sk       int64,
    cr_ship_mode_sk          int64,
    cr_warehouse_sk          int64,
    cr_reason_sk             int64,
    cr_order_number          int64,
    cr_return_quantity       int64,
    cr_return_amount         number(10,2),
    cr_return_tax            number(10,2),
    cr_return_amt_inc_tax    number(10,2),
    cr_fee                   number(10,2),
    cr_return_ship_cost      number(10,2),
    cr_refunded_cash         number(10,2),
    cr_reversed_charge       number(10,2),
    cr_store_credit          number(10,2),
    cr_net_loss              number(10,2),
        d_empty                  string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file catalog_returns_dataset
(
    schema:catalog_returns_schema,
    files:(
            (filename:"/tpcds/data100G/catalog_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create hyper table if not exists catalog_returns using catalog_returns_schema;

create map if not exists catalog_returns_map_cr_returned_date_sk on catalog_returns
KEY (cr_order_number)
VALUE (cr_returned_date_sk);


create job catalog_returns_job()
begin
create dataproc load_data catalog_returns_doc
(
    input:catalog_returns_dataset,
    table:catalog_returns
);
end;
run job catalog_returns_job(threadnum:6,processnum:3);
