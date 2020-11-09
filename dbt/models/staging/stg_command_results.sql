with column_renames as (
  select
    EventType as event_type
    ,DateCreated as update_timestamp
    ,MapRevision as map_revision
    ,ItemKey as item_key
    ,ItemRevision as item_revision
    ,EndpointId as twilio_sync_endpoint_id
    ,ItemData as item_data
    ,MapUniqueName as  device_id

    ,_source_file
    ,concat(MapUniqueName, '_', MapRevision, '_', STRING(DateCreated)) as _uid
  from
    interview_source.raw_sync_events
)
,extract_event_values as (
  select
    *

    ,JSON_EXTRACT_SCALAR(item_data, '$.uuid') as event_uuid
    ,JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') as command

    ,case
     when JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(item_data, '$.timestamp') as timestamp)
      when JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(item_data, '$.timestamp'),0,26) as timestamp)
    end as event_timestamp

    ,case when item_key in ('Command') then
      JSON_EXTRACT(item_data, '$.desired_state')
      else null
    end as _raw_command_desired_state
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.client')
      else null
    end as command_client
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.user')
      else null
    end as command_user
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin')
      else null
     end as command_origin
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin_id')
      else null
     end as command_origin_id

    ,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.result')
      else null
    end as command_result
    ,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.failure_string')
      else null
    end as command_failure_string

    ,case when item_key in ('CommandActive', 'CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.node_id')
      else null
    end as command_node_id

    ,case when item_key in ('Command', 'CommandActive', 'CommandResult') then
      case
        when JSON_EXTRACT_SCALAR(item_data, '$.command_id') is not null
        then JSON_EXTRACT_SCALAR(item_data, '$.command_id')
        else JSON_EXTRACT_SCALAR(item_data, '$.uuid')
      end
      else null
    end as command_uuid

  from
    column_renames
)
,final as (
  select
  distinct
    command_uuid

    ,command_failure_string
    ,command_node_id
    ,CASE WHEN command_result in ('true','success') THEN 1
          ELSE 0
          END as is_hub_success
    ,update_timestamp
    ,event_timestamp
   -- ,event_uuid
    ,device_id

    --,_uid
    ,_source_file
  from
    extract_event_values
  where
    item_key = 'CommandResult'
    and command_uuid is not null
)

select *
from final
