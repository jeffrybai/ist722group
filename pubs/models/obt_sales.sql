with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_titles as (
    select *
    EXCLUDE (publishers_key)
    FROM {{ ref("dim_titles") }}
),
d_stores as (
    select * from {{ ref('dim_stores') }}
),
d_publishers as (
    select * from {{ ref('dim_publishers') }}
)

select 
    fs.order_number,
    dt.*,
    ds.*, 
    dp.publisher_name,
    dd.*,
    fs.quantity, 
    fs.price,
    fs.extended_price_amount, 
    fs.royalty_amount
from f_sales as fs
    left join d_titles as dt
        on fs.titles_key = dt.titles_key
    left join d_stores as ds 
        ON fs.stores_key = ds.stores_key
    left join d_publishers dp
        ON fs.publishers_key = dp.publishers_key
    left join d_date as dd
        on fs.orderdate_key = dd.date_key
