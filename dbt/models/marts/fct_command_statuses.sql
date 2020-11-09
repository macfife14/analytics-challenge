with column_renames as (
  select
    EventType as event_type
    ,DateCreated as update_timestamp
    ,MapRevision as map_revision
    ,mapuniquename as map_id
    ,ItemKey as item_key
    ,ItemRevision as item_revision
    ,EndpointId as twilio_sync_endpoint_id
    ,ItemData as item_data
    ,MapUniqueName as  device_id
    ,eventid as event_id

    ,_source_file
    ,concat(MapUniqueName, '_', MapRevision, '_', STRING(DateCreated)) as _uid
  from
    interview_source.raw_sync_events as source
)
,extract_event_values as (
  select
    *

   -- ,JSON_EXTRACT_SCALAR(item_data, '$.uuid') as event_uuid
   -- ,JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') as command
    
    
      ,CONCAT('UUID','_',event_id,'_',map_revision,'_',map_id,'_',item_revision)
       as command_uuid
    
    ,case when item_key = 'Command' then
      JSON_EXTRACT_SCALAR(item_data, '$.client')
      else null
    end as command_client

    ,case
     when item_key = 'Command' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(item_data, '$.timestamp') as timestamp)
      when item_key = 'Command' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(item_data, '$.timestamp'),0,26) as timestamp)
      else null
    end as command_timestamp

    ,case when item_key in ('Command') then
      JSON_EXTRACT(item_data, '$.desired_state')
      else null
    end as _raw_command_desired_state
   
    , JSON_EXTRACT_SCALAR(item_data, '$.user') as user_id
    
   ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin')
      else null
     end as command_origin
     
   ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.uuid')
      else null
     end as command_origin_id
      
   ,case
     when item_key = 'CommandActive' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(item_data, '$.timestamp') as timestamp)
      when item_key = 'CommandActive' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(item_data, '$.timestamp'),0,26) as timestamp)
      else null
    end as active_timestamp
    
    ,case
     when item_key = 'CommandResult' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(item_data, '$.timestamp') as timestamp)
      when item_key = 'CommandResult' AND JSON_EXTRACT_SCALAR(item_data, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(item_data, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(item_data, '$.timestamp'),0,26) as timestamp)
      else null
    end as result_timestamp

   ,case when item_key = 'CommandResult' then
      JSON_EXTRACT_SCALAR(item_data, '$.result')
      else null
    end as command_result
    
    ,case when item_key = 'CommandResult' then
      JSON_EXTRACT_SCALAR(item_data, '$.failure_string')
      else null
    end as command_failure_string

   ,case when item_key = 'CommandActive' then
      JSON_EXTRACT_SCALAR(item_data, '$.node_id')
      else null
    end as command_active_node_id

   ,case when item_key = 'CommandResult' then
      JSON_EXTRACT_SCALAR(item_data, '$.node_id')
      else null
    end as command_result_node_id
    
    
   ,case
     when item_key = 'Command' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') as timestamp)
      when item_key = 'Command' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp'),0,26) as timestamp)
      else null
      end as command_update_timestamp
         
    
   ,case
     when item_key = 'CommandActive' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') as timestamp)
      when item_key = 'CommandActive' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp'),0,26) as timestamp)
      else null
         END AS active_update_timestamp

   ,case
     when item_key = 'CommandResult' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) < 26
      then safe_cast(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') as timestamp)
      when item_key = 'CommandResult' AND JSON_EXTRACT_SCALAR(_source_file, '$.timestamp') is not null and CHAR_LENGTH(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp')) > 26
      then safe_cast(substr(JSON_EXTRACT_SCALAR(_source_file, '$.timestamp'),0,26) as timestamp)
      else null
         END AS result_update_timestamp
         
   ,case when item_key = 'Command' then
      CONCAT('COMMAND-',JSON_EXTRACT_SCALAR(item_data, '$.uuid'))
      else null
    end as _command_uid
    
    ,case when item_key = 'CommandActive'
      THEN CONCAT('ACTIVE-',JSON_EXTRACT_SCALAR(item_data, '$.uuid'))
      else null
    end as _command_active_uid
    
    ,case when item_key = 'CommandResult' then
      CONCAT('RESULT-',JSON_EXTRACT_SCALAR(item_data, '$.uuid'))
      else null
    end as _command_result_uid

    -- NEW COLUMNS PARSING `$.desired_state` from item_data CAN BE ADDED HERE --

-- DIMMER --
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'DimmerState' and JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]') = 'Multilevel On' THEN 'ON'
          when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'DimmerState' and JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]') = 'Multilevel Off' THEN 'OFF'
          ELSE NULL
          END AS dimmer_state
    
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'DimmerState' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[2]')
          ELSE NULL
          END AS dimmer_level
          
 -- THERMOSTAT --
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatHeatSetPoint' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS thermostat_heat_set_point
          
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatCoolSetpoint' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS thermostat_cool_set_point
    
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'ThermostatMode' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS thermostat_mode
          
    
    
 -- PIN --
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'PinAssignment' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS slot
          
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'PinAssignment' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[2]')
          ELSE NULL
          END AS pin
          
 -- SWITCH --
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'Switch' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS switch_state
          
 -- LOCK --         
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'DoorLocked' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
          ELSE NULL
          END AS lock_state
    
  from
    column_renames
)
,final as (
  select 
   command_uuid
  ,command_client
  ,thermostat_heat_set_point
  ,thermostat_cool_set_point
  ,thermostat_mode
  ,slot
  ,pin
  ,switch_state
  ,dimmer_state
  ,dimmer_level
  ,lock_state
  ,user_id
  --,event_timestamp
  ,command_timestamp
  ,active_timestamp
  ,result_timestamp
  ,command_active_node_id
  ,command_result_node_id
  ,CASE WHEN command_result in ('true','success') THEN 1
          ELSE 0
          END as is_hub_success
  ,CASE WHEN command_result = 'FALSE' AND command_failure_string IS NOT NULL THEN 1
          ELSE 0
          END as has_hub_response
  ,command_update_timestamp
  ,active_update_timestamp
  ,result_update_timestamp
  ,command_origin
  ,command_origin_id
  ,_command_uid
  ,_command_active_uid
  ,_command_result_uid
  ,_raw_command_desired_state
  
    
  from
    extract_event_values
  where command_uuid is not null
  and user_id is not null
  
  
)

select *
from final
