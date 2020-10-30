select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('ThermostatCoolSetPoint')
  and (
    thermostat_cool_set_point IS NULL
    or NOT REGEXP_CONTAINS(thermostat_cool_set_point, '^([0-9]{1,3}\\.?[0-9]{1,3})$')
  )
limit 1