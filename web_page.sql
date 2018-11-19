use db;
  
create data schema if not exists web_page_schema
TYPE CSV
FIELDS(
wp_web_page_sk            type int64,
wp_web_page_id            type string,
wp_rec_start_date         type datetime format "%Y-%m-%d %H:%M:%S",
wp_rec_end_date           type datetime format "%Y-%m-%d %H:%M:%S",
wp_creation_date_sk       type int64,
wp_access_date_sk         type int64,
wp_autogen_flag           type string,
wp_customer_sk            type int64,
wp_url                    type string,
wp_type                   type string,
wp_char_count             type int64,
wp_link_count             type int64,
wp_image_count            type int64,
wp_max_ad_count           type int64,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists web_page_table using web_page_schema;

create map if not exists web_page_map_wp_char_count on web_page_table
KEY (wp_web_page_sk)
VALUE (wp_char_count);

create job web_page_job(2)
begin
create dataset file web_page_dataset
(
    schema:web_page_schema,
    files:(
            (filename:"/tpcds/data1G/web_page.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
