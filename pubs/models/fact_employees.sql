select
    {{ dbt_utils.generate_surrogate_key(['e.emp_id', 'e.job_id', 'e.pub_id']) }} as assignment_key,
    {{ dbt_utils.generate_surrogate_key(['e.emp_id']) }} as employee_key,
    {{ dbt_utils.generate_surrogate_key([pad_publisher_key('PUB_ID')]) }} as publisher_key,
    {{ dbt_utils.generate_surrogate_key(['e.job_id']) }} as job_key,
    case
        when minit is null or minit = '' 
            then concat(fname, ' ', lname)
        else 
            concat(fname, ' ', minit, ' ', lname)
    end as employee_fullname,
    to_char(e.HIRE_DATE,'YYYYMMDD') as HIRE_DATE_KEY
from
    {{ source('pubs', 'Employee') }} as e