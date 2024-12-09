WITH f_royal_disbursement AS (
    SELECT * FROM {{ ref("fact_royalty_disbursement") }}
),
d_title AS (
 SELECT * 
 FROM {{ ref("dim_titles") }}
),
d_author AS (
    SELECT * FROM {{ ref("dim_authors") }}
),
d_publishers as ( 
    SELECT * FROM {{ ref("dim_publishers") }}
),
d_date 
AS ( SELECT * FROM {{ ref("dim_date") }}
)
SELECT 
    d_title.*, 
    d_author.*, 
    f.quantity_sold_per_year,
    f.total_sales,
    f.royalty_per_author,
    f.royalty_per_title,
    f.order_year
FROM f_royal_disbursement AS f
LEFT JOIN d_title ON f.titles_key = d_title.titles_key
LEFT JOIN d_author ON f.authors_key = d_author.authors_key