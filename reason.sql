use db;
  
create data schema if not exists reason_schema
TYPE CSV
FIELDS(
r_reason_sk               type int64,
r_reason_id               type string,
r_reason_desc             type string,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists reason_table using reason_schema;

create map if not exists reason_map_r_reason_desc on reason_table
KEY (r_reason_sk)
VALUE (r_reason_desc);

create job reason_job(2)
begin
create dataset file reason_dataset
(
    schema:reason_schema,
    files:(
            (filename:"/tpcds/data1G/reason.dat",
            serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data reason_doc
(
    input:reason_dataset,
    table:reason_table
);

end;

run job reason_job(threadnum:6,processnum:3);
