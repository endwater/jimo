se db;
  
create data schema if not exists store_schema
TYPE CSV
FIELDS(
s_store_sk                type int64,
s_store_id                type string,
s_rec_start_date          type datetime format "%Y-%m-%d %H:%M:%S",
s_rec_end_date            type datetime format "%Y-%m-%d %H:%M:%S",
s_closed_date_sk          type int64,
s_store_name              type string,
s_number_employees        type int64,
s_floor_space             type int64,
s_hours                   type string,
s_manager                 type string,
s_market_id               type int64,
s_geography_class         type string,
s_market_desc             type string,
s_market_manager          type string,
s_division_id             type int64,
s_division_name           type string,
s_company_id              type int64,
s_company_name            type string,
s_street_number           type string,
s_street_name             type string,
s_street_type             type string,
s_suite_number            type string,
s_city                    type string,
s_county                  type string,
s_state                   type string,
s_zip                     type string,
s_country                 type string,
s_gmt_offset              type double,
s_tax_precentage          type double,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists store_table using store_schema;
