with stg_titles as (
    select * from {{ source('pubs','Titles')}}
)
select  
    {{ dbt_utils.generate_surrogate_key(['stg_titles.title_id']) }} as title_key,
    stg_titles.*
from stg_titles