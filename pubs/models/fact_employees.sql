select
    {{ dbt_utils.generate_surrogate_key(['e.emp_id', 'e.job_id', 'e.pub_id']) }} as assignment_key,
    {{ dbt_utils.generate_surrogate_key(['e.emp_id']) }} as employee_key,
    {{ dbt_utils.generate_surrogate_key(['e.pub_id']) }} as publisher_key,
    {{ dbt_utils.generate_surrogate_key(['e.job_id']) }} as job_key,
    e.hire_date
from
    {{ source('pubs', 'Employee') }} e