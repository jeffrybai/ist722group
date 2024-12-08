WITH stg_titles AS (
    SELECT * 
    FROM {{ source('pubs', 'Titles') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_titles.title_id']) }} AS titles_key,
    {{ dbt_utils.generate_surrogate_key(['stg_titles.pub_id']) }} AS publishers_key,
    title_id,
    title,
    type AS title_category,
    pub_id AS publisher_id,
    price AS title_price,
    royalty AS title_royalty,
    ytd_sales AS title_ytd_sales,
     EXTRACT(YEAR FROM pubdate) AS published_year,
    EXTRACT(MONTH FROM pubdate) AS published_month,
    EXTRACT(DAY FROM pubdate) AS published_day
FROM stg_titles