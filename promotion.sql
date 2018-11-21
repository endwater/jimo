use db;
  
create data schema if not exists promotion_schema
TYPE CSV
FIELDS(
p_promo_sk                type int64,
p_promo_id                type string,
p_start_date_sk           type int64,
p_end_date_sk             type int64,
p_item_sk                 type int64,
p_cost                    type double,
p_response_target         type int64,
p_promo_name              type string,
p_channel_dmail           type string,
p_channel_email           type string,
p_channel_catalog         type string,
p_channel_tv              type string,
p_channel_radio           type string,
p_channel_press           type string,
p_channel_event           type string,
p_channel_demo            type string,
p_channel_details         type string,
p_purpose                 type string,
p_discount_active         type string,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists promotion_table using promotion_schema;

create map if not exists promotion_map_p_channel_dmail on promotion_table
KEY (p_promo_sk)
VALUE (p_channel_dmail);

create map if not exists promotion_map_p_channel_email on promotion_table
KEY (p_promo_sk)
VALUE (p_channel_email);

create map if not exists promotion_map_p_channel_tv on promotion_table
KEY (p_promo_sk)
VALUE (p_channel_tv);


create job promotion_job(2)
begin
create dataset file promotion_dataset
(
    schema:promotion_schema,
    files:(
            (filename:"/tpcds/data1G/promotion.dat",
            serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data promotion_doc
(
    input:promotion_dataset,
    table:promotion_table
);

end;

run job promotion_job(threadnum:6,processnum:3);
