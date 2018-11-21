use db;
  
create data schema if not exists date_dim_schema
TYPE CSV
FIELDS(
d_date_sk                 type int64,
d_date_id                 type string,
d_date                    type datetime format "%Y-%m-%d %H:%M:%S",
d_month_seq               type int64,
d_week_seq                type int64,
d_quarter_seq             type int64,
d_year                    type int64,
d_dow                     type int64,
d_moy                     type int64,
d_dom                     type int64,
d_qoy                     type int64,
d_fy_year                 type int64,
d_fy_quarter_seq          type int64,
d_fy_week_seq             type int64,
d_day_name                type string,
d_quarter_name            type string,
d_holiday                 type string,
d_weekend                 type string,
d_following_holiday       type string,
d_first_dom               type int64,
d_last_dom                type int64,
d_same_day_ly             type int64,
d_same_day_lq             type int64,
d_current_day             type string,
d_current_week            type string,
d_current_month           type string,
d_current_quarter         type string,
d_current_year            type string,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists date_dim_table using date_dim_schema;

create map if not exists date_dim_map_d_date on date_dim_table
KEY (d_date_sk)
VALUE (d_date);

create map if not exists date_dim_map_d_month_seq on date_dim_table
KEY (d_date_sk)
VALUE (d_month_seq);

create map if not exists date_dim_map_d_week_seq on date_dim_table
KEY (d_date_sk)
VALUE (d_week_seq);

create map if not exists date_dim_map_d_quarter_seq on date_dim_table
KEY (d_date_sk)
VALUE (d_quarter_seq);

create map if not exists date_dim_map_d_year on date_dim_table
KEY (d_date_sk)
VALUE (d_year);

create map if not exists date_dim_map_d_dow on date_dim_table
KEY (d_date_sk)
VALUE (d_dow);

create map if not exists date_dim_map_d_moy on date_dim_table
KEY (d_date_sk)
VALUE (d_moy);

create map if not exists date_dim_map_d_dom on date_dim_table
KEY (d_date_sk)
VALUE (d_dom);

create map if not exists date_dim_map_d_qoy on date_dim_table
KEY (d_date_sk)
VALUE (d_qoy);

create map if not exists date_dim_map_d_day_name on date_dim_table
KEY (d_date_sk)
VALUE (d_day_name);

create map if not exists date_dim_map_d_quarter_name on date_dim_table
KEY (d_date_sk)
VALUE (d_quarter_name);


create job date_dim_job(1)
begin
create dataset file date_dim_dataset
(
    schema:date_dim_schema,
    files:(
            (filename:"/tpcds/data1G/date_dim.dat",
            serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data date_dim_doc
(
    input:date_dim_dataset,
    table:date_dim_table
);
end;

run job date_dim_job(threadnum:6,processnum:3);
