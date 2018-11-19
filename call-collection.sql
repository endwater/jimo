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
