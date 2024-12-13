with stg_stores as (
    select * ,
    {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,  
    from {{ source('pubs', 'Stores') }}
),
stg_discounts as (
    select 
       {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,
        discounttype,
        discount
    from {{ source('pubs', 'Discounts') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key,
    st.stor_id,                                               
    st.stor_name as store_name,                                           
    st.stor_address as store_address,                                     
    st.city as store_city,                                                
    st.state as store_state,                                              
    st.zip as store_zip,
    coalesce(ds.discounttype,'No Discount') as store_discounttype,
    coalesce(ds.discount,0.0) as store_discountpercent                                 
from stg_stores as st
left join stg_discounts ds on st.stores_key = ds.stores_key
