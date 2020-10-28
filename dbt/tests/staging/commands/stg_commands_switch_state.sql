select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('SwitchState')
  and (
      switch_state is null
      or switch_state not in ('on', 'off')
  )
limit 1