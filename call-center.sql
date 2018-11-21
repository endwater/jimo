create map if not exists call_center_map_cc_county on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_county);

create map if not exists cc_map_cc_call_center_id on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_call_center_id);

create map if not exists cc_map_cc_name on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_name);

create map if not exists cc_map_cc_manager on call_center_table
KEY (cc_call_center_sk)
VALUE (cc_manager);

create job call_center_job(2)
begin
create dataset file call_center_dataset
(
    schema:call_center_schema,
    files:(
            (filename:"/tpcds/data1G/call_center.dat",serverid:0)
            ),
    splitter:(
                block_size:10000
            )
);

create dataproc load_data call_center_doc
(
    input:call_center_dataset,
    table:call_center_table
);

end;

run job call_center_job(threadnum:6,processnum:3);
