create database if not exists db;
use db;

create data schema if not exists store_returns_schema
TYPE CSV
FIELDS(
        sr_returned_date_sk      int64,
        sr_return_time_sk        int64,
        sr_item_sk               int64,
        sr_customer_sk           int64,
        sr_cdemo_sk              int64,
        sr_hdemo_sk              int64,
        sr_addr_sk               int64,
        sr_store_sk              int64,
        sr_reason_sk             int64,
        sr_ticket_number         int64,
        sr_return_quantity       int64,
        sr_return_amt            number(10,2),
        sr_return_tax            number(10,2),
        sr_return_amt_inc_tax    number(10,2),
        sr_fee                   number(10,2),
        sr_return_ship_cost      number(10,2),
        sr_refunded_cash         number(10,2),
        sr_reversed_charge       number(10,2),
        sr_store_credit          number(10,2),
        sr_net_loss              number(10,2),
        d_empty                  string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file store_returns_dataset
(
    schema:store_returns_schema,
    files:(
            (filename:"/tpcds/data100G/store_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create hyper table if not exists store_returns using store_returns_schema;


create job store_returns_job(1)
begin
create dataproc load_data store_returns_doc
(
    input:store_returns_dataset,
    table:store_returns
);
end;
run job store_returns_job(threadnum:6,processnum:3);
