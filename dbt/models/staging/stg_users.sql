with final as (
  select
    *
  from
    {{ source('interview_source', 'raw_users') }}
)

select
  *
from
  final