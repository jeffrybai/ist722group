
select 
    {{ dbt_utils.generate_surrogate_key(['pub_id']) }} as publishers_key,
    *
   
 from {{ source('pubs', 'Publishers') }}


