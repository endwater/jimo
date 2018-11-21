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

create map if not exists store_map_s_store_name on store_table
KEY (s_store_sk)
VALUE (s_store_name);

create map if not exists store_map_s_city on store_table
KEY (s_store_sk)
VALUE (s_city);

create map if not exists store_map_s_county on store_table
KEY (s_store_sk)
VALUE (s_county);

create map if not exists store_map_s_zip on store_table
KEY (s_store_sk)
VALUE (s_zip);

create map if not exists store_map_s_state on store_table
KEY (s_store_sk)
VALUE (s_state);

create map if not exists store_map_s_country on store_table
KEY (s_store_sk)
VALUE (s_country);

create map if not exists store_map_s_gmt_offset on store_table
KEY (s_store_sk)
VALUE (s_gmt_offset);

create map if not exists store_map_s_market_id on store_table
KEY (s_store_sk)
VALUE (s_market_id);

create job store_job(2)
begin
create dataset file store_dataset
(
    schema:store_schema,
    files:(
            (filename:"/tpcds/data1G/store.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data store_doc
(
    input:store_dataset,
    table:store_table
);

end;

run job store_job(threadnum:6,processnum:3);
