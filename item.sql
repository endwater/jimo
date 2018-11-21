use db;
  
create data schema if not exists item_schema
TYPE CSV
FIELDS(
i_item_sk                 type int64,
i_item_id                 type string,
i_rec_start_date          type datetime format "%Y-%m-%d %H:%M:%S",
i_rec_end_date            type datetime format "%Y-%m-%d %H:%M:%S",
i_item_desc               type string,
i_current_price           type number(10,2),
i_wholesale_cost          type number(10,2),
i_brand_id                type int64,
i_brand                   type string,
i_class_id                type int64,
i_class                   type string,
i_category_id             type int64,
i_category                type string,
i_manufact_id             type int64,
i_manufact                type string,
i_size                    type string,
i_formulation             type string,
i_color                   type string,
i_units                   type string,
i_container               type string,
i_manager_id              type int64,
i_product_name            type string,
d_empty                   type string
)
FIELDS TERMINATED BY "|"
ENCLOSED BY "NULL"
LINES TERMINATED BY "LF";

create table if not exists item_table using item_schema;

create map if not exists item_map_i_item_id on item_table
KEY (i_item_sk)
VALUE (i_item_id);

create map if not exists item_map_i_item_desc on item_table
KEY (i_item_sk)
VALUE (i_item_desc);

create map if not exists item_map_i_current_price on item_table
KEY (i_item_sk)
VALUE (i_current_price);

create map if not exists item_map_i_wholesale_cost on item_table
KEY (i_item_sk)
VALUE (i_wholesale_cost);

create map if not exists item_map_i_brand_id on item_table
KEY (i_item_sk)
VALUE (i_brand_id);

create map if not exists item_map_i_brand on item_table
KEY (i_item_sk)
VALUE (i_brand);

create map if not exists item_map_i_class_id on item_table
KEY (i_item_sk)
VALUE (i_class_id);

create map if not exists item_map_i_class on item_table
KEY (i_item_sk)
VALUE (i_class);

create map if not exists item_map_i_category_id on item_table
KEY (i_item_sk)
VALUE (i_category_id);

create map if not exists item_map_i_category on item_table
KEY (i_item_sk)
VALUE (i_category);

create map if not exists item_map_i_manufact_id on item_table
KEY (i_item_sk)
VALUE (i_manufact_id);

create map if not exists item_map_i_manufact on item_table
KEY (i_item_sk)
VALUE (i_manufact);

create map if not exists item_map_i_size on item_table
KEY (i_item_sk)
VALUE (i_size);

create map if not exists item_map_i_formulation on item_table
KEY (i_item_sk)
VALUE (i_formulation);

create map if not exists item_map_i_color on item_table
KEY (i_item_sk)
VALUE (i_color);

create map if not exists item_map_i_units on item_table
KEY (i_item_sk)
VALUE (i_units);

create map if not exists item_map_i_container on item_table
KEY (i_item_sk)
VALUE (i_container);

create map if not exists item_map_i_manager_id on item_table
KEY (i_item_sk)
VALUE (i_manager_id);

create map if not exists item_map_i_product_name on item_table
KEY (i_item_sk)
VALUE (i_product_name);

create job item_job(2)
begin
create dataset file item_dataset
(
    schema:item_schema,
    files:(
            (filename:"/tpcds/data1G/item.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data item_doc
(
    input:item_dataset,
    table:item_table
);

end;

run job item_job(threadnum:6,processnum:3);
