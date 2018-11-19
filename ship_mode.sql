use db;
create data schema if not exists ship_mode_schema
TYPE CSV
FIELDS(
sm_ship_mode_sk           type int64,
sm_ship_mode_id           type string,
sm_type                   type string,
sm_code                   type string,
sm_carrier                type string,
sm_contract               type string,
d_empty                                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists ship_mode_table using ship_mode_schema;

create map if not exists ship_mode_map_sm_type on ship_mode_table
KEY (sm_ship_mode_sk)
VALUE (sm_type);

create map if not exists ship_mode_map_sm_code on ship_mode_table
KEY (sm_ship_mode_sk)
VALUE (sm_code);

create map if not exists ship_mode_map_sm_contract on ship_mode_table
KEY (sm_ship_mode_sk)
VALUE (sm_contract);

create job ship_mode_job(2)
begin
create dataset file ship_mode_dataset
(
    schema:ship_mode_schema,
    files:(
            (filename:"/tpcds/data1G/ship_mode.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
