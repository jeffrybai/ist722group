with stg_titles as (
    select * from {{ source('pubs','Titles')}}
),
stg_titleauthor as (
    select
        au_id as title_author_id,
        ta.title_id
    from {{ source('pubs','TitleAuthor')}}
)
select  
    {{ dbt_utils.generate_surrogate_key(['title_id']) }} as title_key, 
    t.title_id,
    t.title,
    t.type,
    t.pub_id,
    t.price,
    t.royalty,
    t.ytd_sales
from stg_titles t
    left join  stg_titleauthor ta on ta.title_id = t.title_id
