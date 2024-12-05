with stg_authors as (
    select * from {{ source('pubs', 'Authors') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['AU_ID']) }} as authors_key,    
    AU_ID as authors_au_id,                                             
    au_fname as authors_au_fname,                                       
    au_lname as authors_au_lname,                                       
    phone as authors_phone,                                             
    address as authors_address,                                         
    city as authors_city,                                               
    state as authors_state,                                             
    zip as authors_zip,                                                 
    contract as authors_contract                                        
from stg_authors



