with stg_authors as (
    select * from {{ source('pubs', 'Authors') }}
)
 
select
    {{ dbt_utils.generate_surrogate_key(['AU_ID']) }} as authors_key,    
    AU_ID AS author_id,                                            
     concat(au_fname , ' ', au_lname) as author_name,                                   
    phone as author_phone,                                            
    address as author_address,                                        
    city as author_city,                                              
    state as author_state,                                            
    zip as author_zip,                                                                                       
from stg_authors


