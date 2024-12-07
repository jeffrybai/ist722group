with stg_titles as (
    select 
        {{dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key,
        title_id,
        title as title_title,
        pub_id,
        price as title_price,
        ytd_sales as title_ytd_sales
    from {{source('pubs','Titles') }}
),
stg_publishers as (
    select
        {{dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key,
        pub_id,
        pub_name
    from {{source('pubs','Publishers') }}
),
stg_stores as (
    select 
        {{dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,
        stor_id,
        stor_name
    from {{ source('pubs','Stores') }}
),
stg_sales as (
    select
        ord_num,
        {{dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,
        title_id,
        ord_date as order_date,
        qty as quantity
    from {{source ('pubs', 'Sales') }}
    group by ord_num, ord_date, title_id, stor_id,qty
),

select
    s.ord_num,
    t.titles_key,
    p.publishers_key,
    s.stores_key,
    s.order_date
    quantity,
    title_title,
    title_price,
    title_ytd_sales,
    (s.quantity * t.title_price) as extended_price_amount,
    (s.quantity * t.title_price * d.discount) as discount_amount,
    ((s.quantity * t.title_price) - (s.quantity * t.title_price * d.discount)) as net_sales_amount,
    ((s.quantity * t.title_price) * ta.royaltyper) as total_royalty_amount
from raw.pubs.sales s
join raw.pubs.titles t on s.title_id = t.title_id
join raw.pubs.publishers p on t.pub_id  = p.pub_id
join raw.pubs.stores st on s.stores_key= st.stores_key
left join {{ source('pubs','Discounts') }} d on s.stores_key = d.stor_id
left join {{ source('pubs','TitleAuthor') }} ta on t.title_id = ta.title_id