
select 
    {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key,
    pub_id as publisher_id ,                                                                            
    pub_name as publisher_name,                                                                                    
    city as publisher_city,                                              
    state as publisher_state,                                            
    country as publisher_country,  
   
 from {{ source('pubs', 'Publishers') }}