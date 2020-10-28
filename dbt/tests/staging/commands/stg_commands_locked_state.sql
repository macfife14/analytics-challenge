select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('LockedState')
  and (
    lock_state is null
    or lock_state NOT IN ('locked', 'unlocked')
  )
limit 1