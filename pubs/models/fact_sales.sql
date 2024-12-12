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
stg_roysched as(
    select *,
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key,                                                   
    from  {{ source('pubs', 'RoySched')}}
),

--Annual sales calc
stg_annual_sales as (
    select distinct
        s.title_id,
        d.order_year,
        sum(s.qty * t.price) as annual_sales
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join stg_date d on s.orderdate_key = d.orderdate_key
    group by s.title_id, d.order_year
),
stg_royalty as (
    select distinct
        a.title_id,
        a.order_year,
        a.annual_sales,
        rs.royalty as royalty_percentage
    from stg_annual_sales as a
    left join stg_roysched rs
        on a.title_id = rs.title_id
        and a.annual_sales between rs.lorange and rs.hirange
),

--Discounts Staging
stg_customer_discount as (
    select
        s.order_number,
        s.stor_id,
        d.discount as discount_percentage,
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join {{ source('pubs', 'Discounts') }} d
        on s.stor_id = d.stor_id
        and d.DISCOUNTTYPE = 'Customer Discount'
),
stg_initial_discount as (
    select
        s.order_number,
        s.stor_id,
        d.discount as discount_percentage
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join {{ source('pubs', 'Discounts') }} d
        on d.DISCOUNTTYPE = 'Initial Customer'
    where not exists (
        select 1 
        from stg_sales s2
        where s2.stor_id = s.stor_id
          and s2.ord_date < s.ord_date
    )
),
stg_volume_discount as (
    select
        s.order_number,
        s.stor_id,
        d.discount as discount_percentage
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join {{ source('pubs', 'Discounts') }} d
        on d.DISCOUNTTYPE = 'Volume Discount'
        and s.qty between d.LOWQTY and d.HIGHQTY
),
stg_discounts as (
    select
        s.order_number,
        -- Sum discounts across all types for each order_number
        coalesce(cd.discount_percentage, 0) +
        coalesce(id.discount_percentage, 0) +
        coalesce(vd.discount_percentage, 0) as discount_percentage,
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join (
        select order_number, discount_percentage
        from stg_customer_discount
    ) cd on s.order_number = cd.order_number
    left join (
        select order_number, discount_percentage
        from stg_initial_discount
    ) id on s.order_number = id.order_number
    left join (
        select order_number, discount_percentage
        from stg_volume_discount
    ) vd on s.order_number = vd.order_number
)


select distinct
    s.order_number,
    t.titles_key,
    p.publishers_key,
    st.stores_key,
    s.orderdate_key,
    dt.order_year,
    t.title as title_title,
    t.price as title_price,
    s.qty as quantity,
    (s.qty * t.price) as extended_price_amount,
    sd.discount_percentage as discount_percentage,
    round(extended_price_amount * (discount_percentage / 100), 2) as discount_amount,
    (extended_price_amount - discount_amount) as net_sales,
    r.royalty_percentage as annual_royalty_percentage,
from stg_sales as s 
    left join stg_titles as t on s.title_id = t.title_id
    left join stg_publishers as p  on t.pub_id  = p.pub_id
    left join stg_stores as st on s.stor_id = st.stor_id
    left join stg_date as dt on s.orderdate_key = dt.orderdate_key
    left join stg_royalty as r on s.title_id = r.title_id 
        and dt.order_year = r.order_year
    left join stg_discounts sd  on s.order_number = sd.order_number