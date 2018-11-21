use db;
  
create data schema if not exists catalog_page_schema
TYPE CSV
FIELDS (
  cp_catalog_page_sk             type int64,
  cp_catalog_page_id             type string,
  cp_start_date_sk               type int64,
  cp_end_date_sk                 type int64,
  cp_department                  type string,
  cp_catalog_number              type int64,
  cp_catalog_page_number         type int64,
  cp_description                 type string,
  cp_type                        type string,
  d_empty                        type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists catalog_page_table using catalog_page_schema;

create map if not exists catalog_page_map_cp_catalog_page_id on catalog_page_table
KEY (cp_catalog_page_sk)
VALUE (cp_catalog_page_id);


create job catalog_page_job(2)
begin
create dataset file catalog_page_dataset
(
    schema:catalog_page_schema,
    files:(
            (filename:"/tpcds/data1G/catalog_page.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data catalog_page_doc
(
    input:catalog_page_dataset,
    table:catalog_page_table
);

end;

run job catalog_page_job(threadnum:6,processnum:3);
