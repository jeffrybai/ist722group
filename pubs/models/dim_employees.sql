select 
    {{ dbt_utils.generate_surrogate_key(['EMP_ID']) }} as employee_key,
    EMP_ID as employee_id,
    FNAME as employee_firstname,
    MINIT as employee_middle_initial,
    LNAME as employee_lastname,
    to_char(HIRE_DATE,'YYYYMMDD') as HIRE_DATE_KEY,
    {{ dbt_utils.generate_surrogate_key(['JOB_ID']) }} as job_key,
    {{ dbt_utils.generate_surrogate_key([pad_publisher_key('PUB_ID')]) }} as publishers_key
from 
    {{ source('pubs', 'Employee') }}