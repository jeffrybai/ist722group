WITH dim_titles AS (
    SELECT *
    FROM {{ ref('dim_titles') }}
),
dim_authors AS (
    SELECT *
    FROM {{ ref('dim_authors') }}
),
dim_publishers AS (
    SELECT *
    FROM {{ ref('dim_publishers') }}
),
dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),
stg_TitleAuthors AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['au_id']) }} AS authors_key, 
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} AS titles_key, 
        AU_ORD, 
        royaltyper                                            
    FROM {{ source('pubs', 'TitleAuthor') }}
),
stg_sales AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ord_num', 'title_id', 'stor_id']) }} AS order_number, 
        ord_num,
        ord_date,
        to_char(ord_date, 'YYYYMMDD') AS orderdate_key,
        year(ord_date) AS order_year, -- Extract year from order date
        title_id,
        stor_id,
        qty
    FROM {{ source('pubs', 'Sales') }}
)
SELECT 
    dt.titles_key,
    a.authors_key,
    s.order_year, 
    SUM(s.qty) AS quantity_sold_per_year, 
    SUM(s.qty * dt.title_price) AS total_sales, 
    t.royaltyper,
    (SUM(s.qty * dt.title_price) * t.royaltyper / 100) AS royalty_per_author, 
     dt.title_royalty,
    (SUM(s.qty * dt.title_price) * dt.title_royalty / 100) AS royalty_per_title 
FROM stg_TitleAuthors t
LEFT JOIN dim_titles dt ON t.titles_key = dt.titles_key
LEFT JOIN dim_authors a ON t.authors_key = a.authors_key
LEFT JOIN stg_sales s ON dt.title_id = s.title_id
GROUP BY 
    dt.titles_key,
    a.authors_key,
    dt.title_royalty,
    s.order_year,
    t.royaltyper