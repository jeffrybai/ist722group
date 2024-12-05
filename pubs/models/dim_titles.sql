with stg_titles as (
    select * from {{ source('pubs','Titles')}}
)
select  
    {{ dbt_utils.generate_surrogate_key(['stg_titles.title_id']) }} as titles_key,
    title_id,
    title,
    type as title_category,
    pub_id,
    price as title_price,
    royalty as title_royalty,
    ytd_sales as title_ytd_sales
from stg_titles
