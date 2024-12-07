WITH dim_titles AS (
    SELECT 
        title_id, 
        titles_key, 
        title_ytd_sales, 
        title_royalty, 
        title, 
        published_date, 
        publisher_id
    FROM {{ ref('dim_titles') }}
),
dim_authors AS (
    SELECT 
        author_id, 
        authors_key, 
        author_name
    FROM {{ ref('dim_authors') }}
),
dim_publishers AS (
    SELECT 
        publisher_id, 
        publishers_key, 
        publisher_name
    FROM {{ ref('dim_publishers') }}
)
SELECT 
    dt.titles_key,
    dt.title_id,
    a.authors_key,
    a.author_id,
    p.publishers_key,
    p.publisher_id,
    dt.title,
    p.publisher_name,
    dt.published_date,
    a.author_name,
    dt.title_ytd_sales,
    dt.title_royalty,
    (dt.title_ytd_sales * dt.title_royalty / 100) AS royalty_amount_per_title,
    t.royaltyper,
    (dt.title_ytd_sales * t.royaltyper / 100) AS royalty_amount_per_author
FROM RAW.PUBS.TitleAuthor t
LEFT JOIN dim_titles dt ON t.title_id = dt.title_id
LEFT JOIN dim_authors a ON t.au_id = a.author_id
LEFT JOIN dim_publishers p ON dt.publisher_id = p.publisher_id