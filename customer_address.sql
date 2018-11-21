use db;
  
create data schema if not exists customer_address_schema
TYPE CSV
FIELDS(
ca_address_sk             type int64,
ca_address_id             type string,
ca_street_number          type string,
ca_street_name            type string,
ca_street_type            type string,
ca_suite_number           type string,
ca_city                   type string,
ca_county                 type string,
ca_state                  type string,
ca_zip                    type string,
ca_country                type string,
ca_gmt_offset             type double,
ca_location_type          type string,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists customer_address_table using customer_address_schema;

create map if not exists customer_address_map_ca_city on customer_address_table
KEY (ca_address_sk)
VALUE (ca_city);

create map if not exists customer_address_map_ca_county on customer_address_table
KEY (ca_address_sk)
VALUE (ca_county);

create map if not exists customer_address_map_ca_state on customer_address_table
KEY (ca_address_sk)
VALUE (ca_state);

create map if not exists customer_address_map_ca_zip on customer_address_table
KEY (ca_address_sk)
VALUE (ca_zip);

create map if not exists customer_address_map_ca_country on customer_address_table
KEY (ca_address_sk)
VALUE (ca_country);

create map if not exists customer_address_map_ca_gmt_offset on customer_address_table
KEY (ca_address_sk)
VALUE (ca_gmt_offset);

create map if not exists customer_address_map_ca_location_type on customer_address_table
KEY (ca_address_sk)
VALUE (ca_location_type);

create job customer_address_job(2)
begin
create dataset file customer_address_dataset
(
    schema:customer_address_schema,
    files:(
            (filename:"/tpcds/data1G/customer_address.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data customer_address_doc
(
    input:customer_address_dataset,
    table:customer_address_table
);

end;

run job customer_address_job(threadnum:6,processnum:3);
