with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_titles as (
    select *
    EXCLUDE (publishers_key)
    FROM {{ ref('dim_titles') }}
),
d_stores as (
    select * from {{ ref('dim_stores') }}
),
d_publishers as (
    select * from {{ ref('dim_publishers') }}
)

select 
    distinct
    f.order_number,
    f.ord_num,
    d_titles.*,
    d_stores.*,
    d_publishers.*,
    d_date.*,
    f.extended_price_amount, 
    f.high_volume_discount,
    f.initial_order_discount,
    f.total_discount_percentage,
    f.total_discount_amount,
    f.net_sales_amount,
    f.total_royalty_amount,
    f.orders,
    f.customer_discount,
    f.no_customer_discount,
    f.volume_discount,
    f.no_volume_discount,
    f.initial_customer_discount,
    f.no_initial_customer_discount
from f_sales as f
    left join d_titles on f.titles_key = d_titles.titles_key
    left join d_stores ON f.stores_key = d_stores.stores_key
    left join d_publishers ON f.publishers_key = d_publishers.publishers_key
    left join d_date on f.orderdate_key = d_date.date_key
