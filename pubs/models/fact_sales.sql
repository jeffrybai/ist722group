 with stg_titles as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key,
    from {{source('pubs','Titles') }}
),
stg_publishers as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key,
    from {{source('pubs','Publishers') }}
),
stg_stores as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,
    from {{ source('pubs','Stores') }}
),
stg_sales as (
    select  
        {{ dbt_utils.generate_surrogate_key(['ord_num','title_id','stor_id']) }} as order_number, 
        ord_num,
        ord_date,
        to_char(ord_date,'YYYYMMDD') as orderdate_key,
        title_id,
        stor_id,
        qty
    from {{ source ('pubs', 'Sales') }}
),
stg_date as (
    select 
        ord_date,
        to_char(ord_date,'YYYYMMDD') as orderdate_key,
        year(ord_date) as order_year
    from {{ source('pubs', 'Sales')}}
)
select
    distinct s.order_number,
    t.titles_key,
    p.publishers_key,
    st.stores_key,
    s.orderdate_key,
    dt.order_year,
    t.title as title_title,
    s.qty as quantity,
    t.price as title_price,
    (s.qty * t.price) as extended_price_amount,
    
    round(( * t.royalty/100),2) as total_royalty_amount 
from stg_sales as s 
left join stg_titles as t on s.title_id = t.title_id
left join stg_publishers as p on t.pub_id  = p.pub_id
left join stg_stores as st on s.stor_id = st.stor_id
left join stg_date as dt on s.orderdate_key = dt.orderdate_key

