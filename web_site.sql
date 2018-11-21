use db;

create data schema if not exists web_site_schema
TYPE CSV
FIELDS(
web_site_sk               type int64,
web_site_id               type string,
web_rec_start_date        type datetime format "%Y-%m-%d %H:%M:%S",
web_rec_end_date          type datetime format "%Y-%m-%d %H:%M:%S",
web_name                  type string,
web_open_date_sk          type int64,
web_close_date_sk         type int64,
web_class                 type string,
web_manager               type string,
web_mkt_id                type int64,
web_mkt_class             type string,
web_mkt_desc              type string,
web_market_manager        type string,
web_company_id            type int64,
web_company_name          type string,
web_street_number         type string,
web_street_name           type string,
web_street_type           type string,
web_suite_number          type string,
web_city                  type string,
web_county                type string,
web_state                 type string,
web_zip                   type string,
web_country               type string,
web_gmt_offset            type double,
web_tax_percentage        type double,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists web_site_table using web_site_schema;

create map if not exists web_site_map_web_site_id on web_site_table
KEY (web_site_sk)
VALUE (web_site_id);

create map if not exists web_site_map_web_name on web_site_table
KEY (web_site_sk)
VALUE (web_name);

create map if not exists web_site_map_web_company_name on web_site_table
KEY (web_site_sk)
VALUE (web_company_name);

create job web_site_job(2)
begin
create dataset file web_site_dataset
(
    schema:web_site_schema,
    files:(
            (filename:"/tpcds/data1G/web_site.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data web_site_doc
(
    input:web_site_dataset,
    table:web_site_table
);

end;

run job web_site_job(threadnum:6,processnum:3);
