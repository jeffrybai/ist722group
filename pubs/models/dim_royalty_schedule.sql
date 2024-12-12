with stg_roysched as (
    select * from {{ source('pubs', 'RoySched') }}
)

select *,
    {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key,                                                   
from stg_roysched