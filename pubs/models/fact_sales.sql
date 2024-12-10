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
),
stg_annual_sales as (
    select
        s.title_id,
        d.order_year,
        sum(s.qty * t.price) as annual_sales
    from stg_sales as s
    left join stg_titles t on s.title_id = t.title_id
    left join stg_date d on s.orderdate_key = d.orderdate_key
    group by s.title_id, d.order_year
),
stg_royalty as (
    select
        a.title_id,
        a.order_year,
        a.annual_sales,
        rs.royalty as royalty_percentage
    from stg_annual_sales as a
    left join {{ source('pubs', 'RoySched') }} rs
        on a.title_id = rs.title_id
        and a.annual_sales between rs.lorange and rs.hirange
)

select distinct
    s.order_number,
    t.titles_key,
    p.publishers_key,
    st.stores_key,
    s.orderdate_key,
    dt.order_year,
    s.qty as quantity,
    t.price as price,
    (s.qty * t.price) as extended_price_amount,
    r.royalty_percentage as annual_royalty_percentage,
    round((extended_price_amount * annual_royalty_percentage/100),2) as royalty_amount 
from stg_sales as s 
    left join stg_titles as t 
        on s.title_id = t.title_id
    left join stg_publishers as p 
        on t.pub_id  = p.pub_id
    left join stg_stores as st 
        on s.stor_id = st.stor_id
    left join stg_date as dt 
        on s.orderdate_key = dt.orderdate_key
    left join stg_royalty as r
        on s.title_id = r.title_id 
        and dt.order_year = r.order_year