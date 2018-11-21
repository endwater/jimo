use db;
  
create data schema if not exists household_demographics_schema
TYPE CSV
FIELDS(
hd_demo_sk                type int64,
hd_income_band_sk         type int64,
hd_buy_potential          type string,
hd_dep_count              type int64,
hd_vehicle_count          type int64,
d_empty                                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists household_demographics_table using household_demographics_schema;

create map if not exists hd_map_hd_buy_potential on household_demographics_table
KEY (hd_demo_sk)
VALUE (hd_buy_potential);

create job household_demographics_job(2)
begin
create dataset file household_demographics_dataset
(
    schema:household_demographics_schema,
    files:(
            (filename:"/tpcds/data1G/household_demographics.dat",
            serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data household_demographics_doc
(
    input:household_demographics_dataset,
    table:household_demographics_table
);

end;

run job household_demographics_job(threadnum:6,processnum:3);
