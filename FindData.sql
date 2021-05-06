--%sql
select payload.data, cast(get_json_object(payload.data, '$.facts.mail.messageId') as varchar(1000)) as messageId
from np_test2_bronze_db_streaming_events.tbl_customer_streaming_bronze
where 1=1
  and cast(get_json_object(payload.data, '$.facts.mail.messageId') as varchar(1000)) = '01020176e16b6a48-c87f96f7-0554-4518-a520-3bb6b280b113-000000'
  and dataset_id = 'ses_email_tracking'
  and partition_date >= '2021-01-08'


SELECT * from messageId