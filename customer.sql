use db;
  
create data schema if not exists customer_schema
TYPE CSV
FIELDS(
c_customer_sk             type int64,
c_customer_id             type string,
c_current_cdemo_sk        type int64,
c_current_hdemo_sk        type int64,
c_current_addr_sk         type int64,
c_first_shipto_date_sk    type int64,
c_first_sales_date_sk     type int64,
c_salutation              type string,
c_first_name              type string,
c_last_name               type string,
c_preferred_cust_flag     type string,
c_birth_day               type int64,
c_birth_month             type int64,
c_birth_year              type int64,
c_birth_country           type string,
c_login                   type string,
c_email_address           type string,
c_last_review_date        type string,
d_empty                                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists customer_table using customer_schema;

create map if not exists customer_map_c_current_cdemo_sk on customer_table
KEY (c_customer_sk)
VALUE (c_current_cdemo_sk);

create map if not exists customer_map_c_current_hdemo_sk on customer_table
KEY (c_customer_sk)
VALUE (c_current_hdemo_sk);

create map if not exists customer_map_c_current_addr_sk on customer_table
KEY (c_customer_sk)
