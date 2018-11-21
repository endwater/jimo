create database if not exists db;
use db;


create data schema if not exists web_returns_schema
TYPE CSV
FIELDS(
        wr_returned_date_sk      int64,
        wr_returned_time_sk      int64,
        wr_item_sk               int64,
        wr_refunded_customer_sk  int64,
        wr_refunded_cdemo_sk     int64,
        wr_refunded_hdemo_sk     int64,
        wr_refunded_addr_sk      int64,
        wr_returning_customer_sk int64,
        wr_returning_cdemo_sk    int64,
        wr_returning_hdemo_sk    int64,
        wr_returning_addr_sk     int64,
        wr_web_page_sk           int64,
        wr_reason_sk             int64,
        wr_order_number          int64,
        wr_return_quantity       int64,
        wr_return_amt            number(10,2),
        wr_return_tax            number(10,2),
        wr_return_amt_inc_tax    number(10,2),
        wr_fee                   number(10,2),
        wr_return_ship_cost      number(10,2),
        wr_refunded_cash         number(10,2),
        wr_reversed_charge       number(10,2),
        wr_account_credit        number(10,2),
        wr_net_loss              number(10,2),
        d_empty                  string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file web_returns_dataset
(
    schema:web_returns_schema,
    files:(
            (filename:"/tpcds/data100G/web_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create hyper table if not exists web_returns using web_returns_schema;


create job web_returns_job(1)
begin
create dataproc load_data web_returns_doc
(
    input:web_returns_dataset,
    table:web_returns
);
end;
run job web_returns_job(threadnum:6,processnum:3);
