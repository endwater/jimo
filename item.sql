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
