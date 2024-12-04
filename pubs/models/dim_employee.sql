with stg_employees as (
    select * from {{ source('pubs', 'Employee') }}
),

stg_jobs as (
    select * from {{ source('pubs', 'Jobs') }}
),

stg_publishers as (
    select * from {{ source('pubs', 'Publishers') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['e.EMP_ID']) }} as employee_key,
    e.EMP_ID as employee_id,
    e.FNAME as employee_firstname,
    e.MINIT as employee_middle_initial,
    e.LNAME as employee_lastname,
    e.HIRE_DATE,
    j.JOB_DESC,
    p.pub_name as employee_company
from 
        stg_employees e 
    left join stg_jobs j 
        on e.job_id = j.job_id
    left join stg_publishers p
        on e.pub_id = p.pub_id