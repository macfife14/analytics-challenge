select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('ThermostatHeatSetPoint', 'ThermostatCoolSetPoint')
  and thermostat_set_point is null
  and (
    thermostat_set_point IS NULL
    or NOT REGEXP_CONTAINS(thermostat_set_point, '^([0-9]{1,3}\\.?[0-9]{1,3})$')
  )
limit 1