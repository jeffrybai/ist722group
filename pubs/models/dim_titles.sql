WITH stg_titles AS (
    SELECT * 
    FROM {{ source('pubs', 'Titles') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['title_id']) }} AS titles_key,
    {{ dbt_utils.generate_surrogate_key(['pub_id']) }} AS publishers_key,
    title_id,
    title as title_title,
    type AS title_category,
    price AS title_price,
    royalty AS title_royalty,
    pubdate as published_year
FROM stg_titles