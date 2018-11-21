create database if not exists db;
use db;

create data schema if not exists store_sales_schema
TYPE CSV
FIELDS(
        ss_sold_date_sk           int64,
        ss_sold_time_sk           int64,
        ss_item_sk                int64,
        ss_customer_sk            int64,
        ss_cdemo_sk               int64,
        ss_hdemo_sk               int64,
        ss_addr_sk                int64,
        ss_store_sk               int64,
        ss_promo_sk               int64,
        ss_ticket_number          int64,
        ss_quantity               int64,
        ss_wholesale_cost         number(10,2),
        ss_list_price             number(10,2),
        ss_sales_price            number(10,2),
        ss_ext_discount_amt       number(10,2),
        ss_ext_sales_price        number(10,2),
        ss_ext_wholesale_cost     number(10,2),
        ss_ext_list_price         number(10,2),
        ss_ext_tax                number(10,2),
        ss_coupon_amt             number(10,2),
        ss_net_paid               number(10,2),
        ss_net_paid_inc_tax       number(10,2),
        ss_net_profit             number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";


create dataset file store_sales_dataset
(
    schema:store_sales_schema,
    files:(
            (filename:"/tpcds/data1G/store_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create data schema if not exists store_returns_schema
TYPE CSV
FIELDS(
        sr_returned_date_sk       int64,
        sr_return_time_sk         int64,
        sr_item_sk                int64,
        sr_customer_sk            int64,
        sr_cdemo_sk               int64,
        sr_hdemo_sk               int64,
        sr_addr_sk                int64,
        sr_store_sk               int64,
        sr_reason_sk              int64,
        sr_ticket_number          int64,
        sr_return_quantity        int64,
        sr_return_amt             number(10,2),
        sr_return_tax             number(10,2),
        sr_return_amt_inc_tax     number(10,2),
        sr_fee                    number(10,2),
        sr_return_ship_cost       number(10,2),
        sr_refunded_cash          number(10,2),
        sr_reversed_charge        number(10,2),
        sr_store_credit           number(10,2),
        sr_net_loss               number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file store_returns_dataset
(
    schema:store_returns_schema,
    files:(
            (filename:"/tpcds/data1G/store_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create data schema if not exists catalog_sales_schema
TYPE CSV
FIELDS(
        cs_sold_date_sk           int64,
        cs_sold_time_sk           int64,
        cs_ship_date_sk           int64,
        cs_bill_customer_sk       int64,
        cs_bill_cdemo_sk          int64,
        cs_bill_hdemo_sk          int64,
        cs_bill_addr_sk           int64,
        cs_ship_customer_sk       int64,
        cs_ship_cdemo_sk          int64,
        cs_ship_hdemo_sk          int64,
        cs_ship_addr_sk           int64,
        cs_call_center_sk         int64,
        cs_catalog_page_sk        int64,
        cs_ship_mode_sk           int64,
        cs_warehouse_sk           int64,
        cs_item_sk                int64,
        cs_promo_sk               int64,
        cs_order_number           int64,
        cs_quantity               int64,
        cs_wholesale_cost         number(10,2),
        cs_list_price             number(10,2),
        cs_sales_price            number(10,2),
        cs_ext_discount_amt       number(10,2),
        cs_ext_sales_price        number(10,2),
        cs_ext_wholesale_cost     number(10,2),
        cs_ext_list_price         number(10,2),
        cs_ext_tax                number(10,2),
        cs_coupon_amt             number(10,2),
        cs_ext_ship_cost          number(10,2),
        cs_net_paid               number(10,2),
        cs_net_paid_inc_tax       number(10,2),
        cs_net_paid_inc_ship      number(10,2),
        cs_net_paid_inc_ship_tax  number(10,2),
        cs_net_profit             number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file catalog_sales_dataset
(
    schema:catalog_sales_schema,
    files:(
            (filename:"/tpcds/data1G/catalog_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create data schema if not exists catalog_returns_schema
TYPE CSV
FIELDS(
        cr_returned_date_sk       int64,
        cr_returned_time_sk       int64,
        cr_item_sk                int64,
        cr_refunded_customer_sk   int64,
        cr_refunded_cdemo_sk      int64,
        cr_refunded_hdemo_sk      int64,
        cr_refunded_addr_sk       int64,
        cr_returning_customer_sk  int64,
        cr_returning_cdemo_sk     int64,
        cr_returning_hdemo_sk     int64,
        cr_returning_addr_sk      int64,
        cr_call_center_sk         int64,
        cr_catalog_page_sk        int64,
        cr_ship_mode_sk           int64,
        cr_warehouse_sk           int64,
        cr_reason_sk              int64,
        cr_order_number           int64,
        cr_return_quantity        int64,
        cr_return_amount          number(10,2),
        cr_return_tax             number(10,2),
        cr_return_amt_inc_tax     number(10,2),
        cr_fee                    number(10,2),
        cr_return_ship_cost       number(10,2),
        cr_refunded_cash          number(10,2),
        cr_reversed_charge        number(10,2),
        cr_store_credit           number(10,2),
        cr_net_loss               number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file catalog_returns_dataset
(
    schema:catalog_returns_schema,
    files:(
            (filename:"/tpcds/data1G/catalog_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create data schema if not exists web_sales_schema
TYPE CSV
FIELDS(
        ws_sold_date_sk           int64,
        ws_sold_time_sk           int64,
        ws_ship_date_sk           int64,
        ws_item_sk                int64,
        ws_bill_customer_sk       int64,
        ws_bill_cdemo_sk          int64,
        ws_bill_hdemo_sk          int64,
        ws_bill_addr_sk           int64,
        ws_ship_customer_sk       int64,
        ws_ship_cdemo_sk          int64,
        ws_ship_hdemo_sk          int64,
        ws_ship_addr_sk           int64,
        ws_web_page_sk            int64,
        ws_web_site_sk            int64,
        ws_ship_mode_sk           int64,
        ws_warehouse_sk           int64,
        ws_promo_sk               int64,
        ws_order_number           int64,
        ws_quantity               int64,
        ws_wholesale_cost         number(10,2),
        ws_list_price             number(10,2),
        ws_sales_price            number(10,2),
        ws_ext_discount_amt       number(10,2),
        ws_ext_sales_price        number(10,2),
        ws_ext_wholesale_cost     number(10,2),
        ws_ext_list_price         number(10,2),
        ws_ext_tax                number(10,2),
        ws_coupon_amt             number(10,2),
        ws_ext_ship_cost          number(10,2),
        ws_net_paid               number(10,2),
        ws_net_paid_inc_tax       number(10,2),
        ws_net_paid_inc_ship      number(10,2),
        ws_net_paid_inc_ship_tax  number(10,2),
        ws_net_profit             number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file web_sales_dataset
(
    schema:web_sales_schema,
    files:(
            (filename:"/tpcds/data1G/web_sales.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


create data schema if not exists web_returns_schema
TYPE CSV
FIELDS(
        wr_returned_date_sk       int64,
        wr_returned_time_sk       int64,
        wr_item_sk                int64,
        wr_refunded_customer_sk   int64,
        wr_refunded_cdemo_sk      int64,
        wr_refunded_hdemo_sk      int64,
        wr_refunded_addr_sk       int64,
        wr_returning_customer_sk  int64,
        wr_returning_cdemo_sk     int64,
        wr_returning_hdemo_sk     int64,
        wr_returning_addr_sk      int64,
        wr_web_page_sk            int64,
        wr_reason_sk              int64,
        wr_order_number           int64,
        wr_return_quantity        int64,
        wr_return_amt             number(10,2),
        wr_return_tax             number(10,2),
        wr_return_amt_inc_tax     number(10,2),
        wr_fee                    number(10,2),
        wr_return_ship_cost       number(10,2),
        wr_refunded_cash          number(10,2),
        wr_reversed_charge        number(10,2),
        wr_account_credit         number(10,2),
        wr_net_loss               number(10,2),
        d_empty                   string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create dataset file web_returns_dataset
(
    schema:web_returns_schema,
    files:(
            (filename:"/tpcds/data1G/web_returns.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);


