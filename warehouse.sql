use db;
  
create data schema if not exists warehouse_schema
TYPE CSV
FIELDS(
w_warehouse_sk            type int64,
w_warehouse_id            type string,
w_warehouse_name          type string,
w_warehouse_sq_ft         type int64,
w_street_number           type string,
w_street_name             type string,
w_street_type             type string,
w_suite_number            type string,
w_city                    type string,
w_county                  type string,
w_state                   type string,
w_zip                     type string,
w_country                 type string,
w_gmt_offset              type int64,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists warehouse_table using warehouse_schema;

create map if not exists warehouse_map_w_warehouse_name on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_warehouse_name);

create map if not exists warehouse_map_w_warehouse_sq_ft on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_warehouse_sq_ft);

create map if not exists warehouse_map_w_city on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_city);

create map if not exists warehouse_map_w_county on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_county);

create map if not exists warehouse_map_w_state on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_state);

create map if not exists warehouse_map_w_zip on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_zip);

create map if not exists warehouse_map_w_country on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_country);

create map if not exists warehouse_map_w_gmt_offset on warehouse_table
KEY (w_warehouse_sk)
VALUE (w_gmt_offset);

create job warehouse_job(2)
begin
create dataset file warehouse_dataset
(
    schema:warehouse_schema,
    files:(
            (filename:"/tpcds/data1G/warehouse.dat",
            serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data warehouse_doc
(
    input:warehouse_dataset,
    table:warehouse_table
);
end;

run job warehouse_job(threadnum:6,processnum:3);
