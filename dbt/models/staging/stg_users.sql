with final as (
  select
  distinct
    uid as user_id
    ,username
    ,dateregistered as date_registered
    ,last_modified
  from
    interview_source.raw_users
)

select
  *
from
  final
