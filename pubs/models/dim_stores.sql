with stg_stores as (
    select * from {{ source('pubs', 'Stores') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as stores_key, 
    stor_id as store_id,                                               
    stor_name as store_name,                                           
    stor_address as store_address,                                     
    city as store_city,                                                
    state as store_state,                                              
    zip as store_zip                                                   
from stg_stores
