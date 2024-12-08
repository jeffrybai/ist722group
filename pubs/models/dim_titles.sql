WITH stg_titles AS (
    SELECT * 
    FROM {{ source('pubs', 'Titles') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_titles.title_id']) }} AS titles_key,
    title_id,
    title,
    type AS title_category,
    pub_id AS publisher_id,
    price AS title_price,
    royalty AS title_royalty,
    ytd_sales AS title_ytd_sales,
    pubdate as published_date
FROM stg_titles