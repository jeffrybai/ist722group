with stg_employees as (
    select * from {{ ref('dim_employees') }}
),

stg_jobs as (
    select 
        {{ dbt_utils.generate_surrogate_key(['JOB_ID']) }} as job_key,
        *
    from {{ source('pubs', 'Jobs') }}
),

stg_publishers as (
    select * from {{ ref('dim_publishers') }}
)

select
    e.employee_id,
    e.employee_firstname,
    e.employee_middle_initial,
    e.employee_lastname,
    case
        when employee_middle_initial is null or employee_middle_initial = '' 
            then concat(employee_firstname, ' ', e.employee_lastname)
        else 
            concat(employee_firstname, ' ', employee_middle_initial, ' ', employee_lastname)
    end as employee_fullname,
    e.hire_date,
    j.job_desc,
    p.publisher_name as employee_company_name,
    p.publisher_city as employee_city,
    p.publisher_state as employee_state,
    p.publisher_country as employee_country
from
        stg_employees e 
    left join stg_jobs j 
        on e.job_key = j.job_key
    left join stg_publishers p 
        on e.publisher_key = p.publishers_key