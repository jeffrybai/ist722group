with stg_titles as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key
    from {{source('pubs','Titles') }}
),
stg_publishers as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key
    from {{source('pubs','Publishers') }}
),
stg_stores as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key
    from {{ source('pubs','Stores') }}
),
stg_sales as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['ord_num','title_id','stor_id']) }} as order_number, 
        to_char(ord_date,'YYYYMMDD') as orderdate_key
    from {{ source ('pubs', 'Sales') }}
),
stg_date as (
    select 
        ord_date,
        to_char(ord_date,'YYYYMMDD') as orderdate_key,
        year(ord_date) as order_year
    from {{ source('pubs', 'Sales')}}
),
stg_customer_discount as (
    select
        s.order_number,
        s.stor_id,
        d.discount as discount_percentage,
    from stg_sales s
    left join stg_titles t on s.title_id = t.title_id
    left join {{ source('pubs', 'Discounts') }} as d 
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
        and s.qty >= 50
),
stg_discounts as (
    select
        s.order_number,
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
    s.ord_num,
    t.title_id,
    p.pub_id,
    st.stor_id,
    t.title as title_title,
    t.price as title_price,
    s.qty as quantity,
    round((s.qty * t.price),2) as extended_price_amount,
    coalesce(d.discounttype,'No Customer Discount') as store_discounttype,
    case 
        when s.qty >= 50 then 'Volume Discount' else 'No Volume Discount' end as high_volume_discount,
    case 
        when not exists ( 
            select 1
            from stg_sales s2
            where s2.stor_id = s.stor_id 
                and s2.ord_date < s.ord_date 
        ) then 'Initial Customer Discount' 
        else 'No Initial Customer Discount' 
        end as initial_order_discount,
    coalesce(d.discount, 0) 
        + case when s.qty >= 50 then 6.7 else 0 end
        + case when not exists (
            select 1 
            from stg_sales s2
            where s2.stor_id = s.stor_id
              and s2.ord_date < s.ord_date
          ) then 10.5 else 0 end as total_discount_percentage,    
    round((s.qty * t.price) * (coalesce(d.discount, 0) 
        + case when s.qty >= 50 then 6.7 else 0 end 
        + case when not exists (
            select 1 
            from stg_sales s2
            where s2.stor_id = s.stor_id
              and s2.ord_date < s.ord_date
          ) then 10.5 else 0 end) / 100, 2) as total_discount_amount,
    (extended_price_amount)-(total_discount_amount) as net_sales_amount,      
    round((net_sales_amount * t.royalty/100),2) as total_royalty_amount,
    1 as orders,
    case 
        when store_discounttype = 'Customer Discount' then 1 else 0 end as customer_discount,
    case 
        when store_discounttype = 'No Customer Discount' then 1 else 0 end as no_customer_discount,
    case 
        when high_volume_discount = 'Volume Discount' then 1 else 0 end as volume_discount,
    case 
        when high_volume_discount = 'No Volume Discount' then 1 else 0 end as no_volume_discount,
    case 
        when initial_order_discount = 'Initial Customer Discount' then 1 else 0 end as initial_customer_discount,
    case 
        when initial_order_discount = 'No Initial Customer Discount' then 1 else 0 end as no_initial_customer_discount
from stg_sales as s 
join stg_titles as t on s.title_id = t.title_id
join stg_publishers as p on t.pub_id  = p.pub_id
join stg_stores as st on s.stor_id = st.stor_id
join stg_date as dt on s.orderdate_key = dt.orderdate_key
left join {{ source('pubs','Discounts') }} as d on s.stor_id = d.stor_id
order by titles_key, orderdate_key
