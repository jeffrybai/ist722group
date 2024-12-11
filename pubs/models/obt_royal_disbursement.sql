WITH f_royal_disbursement AS (
    SELECT * FROM {{ ref("fact_royalty_disbursement") }}
),
d_title AS (
 SELECT * EXCLUDE (publishers_key)

 FROM {{ ref("dim_titles") }}
),
d_author AS (
    SELECT * FROM {{ ref("dim_authors") }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
SELECT 
    d_title.*, 
    d_author.*, 
    d_date.*,
    f.order_year, 
    f.royalty_percentage,
    f.total_royalty,
    f.authors_percentage,
    f.royalty_per_author
FROM f_royal_disbursement AS f
LEFT JOIN d_title ON f.titles_key = d_title.titles_key
LEFT JOIN d_author ON f.authors_key = d_author.authors_key
left join d_date on f.order_year = d_date.date_key