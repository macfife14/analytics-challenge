select
  command_uuid
from
  {{ ref('stg_commands') }}
where
  command IN ('PinAssignment')
  and (
    (slot IS NULL or NOT REGEXP_CONTAINS(slot, '^([0-9]{1,3}|None)$'))
    or NOT REGEXP_CONTAINS(pin, '^([0-9]{4,5}|None)$')
  )
limit 1