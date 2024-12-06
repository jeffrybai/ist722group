with stg_titles as 
(
    select 
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key, 
    from {{source('pubs','Sales')}}
),
with stg_publishers
(
    select
        {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key
    from     from {{source('pubs','Publishers')}}
),
with stg_stores
(
    select
         {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stors_key
    from {{source('pubs','Stores')}}
),     
stg_sales as
(
    select 
    ord_num,
    sum(Quantity) as quantity, 
    sum(Quantity*) as extendedpriceamount,
    sum((Quantity*UnitPrice)*Discount) as discountamount,
    sum(((Quantity*UnitPrice))-((Quantity*UnitPrice)*Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by orderid,productkey
)
select 
    stg_sales.ord_num,
    stg_orders.employeekey,
    stg_orders.customerkey,
    stg_orders.orderdatekey,
    stg_order_details.productkey,
    stg_order_details.quantity,
    stg_order_details.extendedpriceamount,
    stg_order_details.discountamount,
    stg_order_details.soldamount
from stg_sales
left join stg_titles on stg_title_id = stg_sales.stg_title_id