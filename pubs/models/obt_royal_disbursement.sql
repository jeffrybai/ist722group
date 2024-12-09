WITH f_royal_disbursement AS (
    SELECT * FROM {{ ref("fact_royalty_disbursement") }}
),
d_title AS (
 SELECT * EXCLUDE (publishers_key, publisher_id)
 FROM {{ ref("dim_titles") }}
),
d_author AS (
    SELECT * FROM {{ ref("dim_authors") }}
),
d_publisher AS (
    SELECT * FROM {{ ref("dim_publishers") }}
),
d_date 
AS ( SELECT * FROM {{ ref("dim_date") }}
)
SELECT 
    d_title.*, 
    d_author.*, 
    d_publisher.*, 
    f.royalty_amount_per_author, 
    f.royalty_amount_per_title
FROM f_royal_disbursement AS f
LEFT JOIN d_title ON f.titles_key = d_title.titles_key
LEFT JOIN d_author ON f.authors_key = d_author.authors_key
LEFT JOIN d_publisher ON f.publishers_key = d_publisher.publishers_key
LEFT JOIN d_date ON d_title.published_year = d_date.YEAR