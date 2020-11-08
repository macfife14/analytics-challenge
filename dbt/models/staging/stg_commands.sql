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
    interview_source.raw_sync_events as source
)
,extract_event_values as (
  select
    *

    ,JSON_EXTRACT_SCALAR(item_data, '$.uuid') as event_uuid
    ,JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') as command
    
    ,case when item_key in ('Command', 'CommandActive', 'CommandResult') then
      case
        when JSON_EXTRACT_SCALAR(item_data, '$.command_id') is not null
        then JSON_EXTRACT_SCALAR(item_data, '$.command_id')
        else JSON_EXTRACT_SCALAR(item_data, '$.uuid')
      end
      else null
    end as command_uuid
    
    ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.client')
      else null
    end as command_client

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
   
    ,CASE WHEN JSON_EXTRACT_SCALAR(item_data, '$.user') IS NOT NULL THEN 
      case 
      when item_key in ('Command') then JSON_EXTRACT_SCALAR(item_data, '$.user')
      end
      else null
    end as user_id
    
   
   /* ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin')
      else null
     end as command_origin*/
     
   /* ,case when item_key in ('Command') then
      JSON_EXTRACT_SCALAR(item_data, '$.origin_id')
      else null
     end as command_origin_id*/

   /* ,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.result')
      else null
    end as command_result*/
    
    /*,case when item_key in ('CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.failure_string')
      else null
    end as command_failure_string*/

   /* ,case when item_key in ('CommandActive', 'CommandResult') then
      JSON_EXTRACT_SCALAR(item_data, '$.node_id')
      else null
    end as command_node_id*/


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
          
    ,case when JSON_EXTRACT_SCALAR(item_data, '$.desired_state[0]') = 'Temperature' THEN JSON_EXTRACT_SCALAR(item_data, '$.desired_state[1]')
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
    *
  from
    extract_event_values
  where
    item_key = 'Command'
    and command_uuid is not null
)

select *
from final
