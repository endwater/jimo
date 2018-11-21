use db;

create data schema if not exists call_center_schema
TYPE CSV
FIELDS(
cc_call_center_sk         type int64,
cc_call_center_id         type string,
cc_rec_start_date         type datetime format "%Y-%m-%d %H:%M:%S",
cc_rec_end_date           type datetime format "%Y-%m-%d %H:%M:%S",
cc_closed_date_sk         type int64,
cc_open_date_sk           type int64,
cc_name                   type string,
cc_class                  type string,
cc_employees              type int64,
cc_sq_ft                  type int64,
cc_hours                  type string,
cc_manager                type string,
cc_mkt_id                 type int64,
cc_mkt_class              type string,
cc_mkt_desc               type string,
cc_market_manager         type string,
cc_division               type int64,
cc_division_name          type string,
cc_company                type int64,
cc_company_name           type string,
cc_street_number          type string,
cc_street_name            type string,
cc_street_type            type string,
cc_suite_number           type string,
cc_city                   type string,
cc_county                 type string,
cc_state                  type string,
cc_zip                    type string,
cc_country                type string,
cc_gmt_offset             type double,
cc_tax_percentage         type double,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists call_center_table using call_center_schema;
cc_employees              type int64,
cc_sq_ft                  type int64,
cc_hours                  type string,
cc_manager                type string,
cc_mkt_id                 type int64,
cc_mkt_class              type string,
cc_mkt_desc               type string,
cc_market_manager         type string,
cc_division               type int64,
cc_division_name          type string,
cc_company                type int64,
cc_company_name           type string,
cc_street_number          type string,
cc_street_name            type string,
cc_street_type            type string,
cc_suite_number           type string,
cc_city                   type string,
cc_county                 type string,
cc_state                  type string,
cc_zip                    type string,
cc_country                type string,
cc_gmt_offset             type double,
cc_tax_percentage         type double,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists call_center_table using call_center_schema;

create map if not exists call_center_map_cc_county on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_county);

create map if not exists cc_map_cc_call_center_id on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_call_center_id);

create map if not exists cc_map_cc_name on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_name);

create map if not exists cc_map_cc_manager on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_manager);

create job call_center_job(2)
begin
create dataset file call_center_dataset
(
    schema:call_center_schema,
    files:(
            (filename:"/tpcds/data1G/call_center.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data call_center_doc
(
    input:call_center_dataset,
    table:call_center_table
);

end;

run job call_center_job(threadnum:6,processnum:3);

