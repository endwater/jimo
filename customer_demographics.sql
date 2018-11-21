use db;
  
create data schema if not exists customer_demographics_schema
TYPE CSV
FIELDS(
cd_demo_sk                type int64,
cd_gender                 type string,
cd_marital_status         type string,
cd_education_status       type string,
cd_purchase_estimate      type int64,
cd_credit_rating          type string,
cd_dep_count              type int64,
cd_dep_employed_count     type int64,
cd_dep_college_count      type int64,
d_empty                                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists customer_demographics_table using customer_demographics_schema;

create map if not exists customer_demographics_map_cd_gender on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_gender);

create map if not exists customer_demographics_map_cd_marital_status on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_marital_status);

create map if not exists customer_demographics_map_cd_education_status on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_education_status);

create map if not exists customer_demographics_map_cd_purchase_estimate on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_purchase_estimate);

create map if not exists customer_demographics_map_cd_credit_rating on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_credit_rating);

create map if not exists customer_demographics_map_cd_dep_count on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_dep_count);

create map if not exists customer_demographics_map_cd_dep_employed_count on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_dep_employed_count);

create map if not exists customer_demographics_map_cd_cd_dep_college_count on customer_demographics_table
KEY (cd_demo_sk)
VALUE (cd_dep_college_count);


create job customer_demographics_job(2)
begin
create dataset file customer_demographics_dataset
(
    schema:customer_demographics_schema,
    files:(
            (filename:"/tpcds/data1G/customer_demographics.dat", serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data customer_demographics_doc
(
    input:customer_demographics_dataset,
    table:customer_demographics_table
);

end;

run job customer_demographics_job(threadnum:6,processnum:3);
                                                             
