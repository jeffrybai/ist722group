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

stg_TitleAuthors as (select AU_ID AS author_id, title_id, AU_ORD, royaltyper
FROM {{ source('pubs', 'TitleAuthor')}}
)

SELECT 
    dt.titles_key,
    a.authors_key,
    p.publishers_key,
    dt.title_id,
    a.author_id,
    p.publisher_id,
    dt.title,
    p.publisher_name,
    dt.published_year,
    a.author_name,
    dt.title_ytd_sales,
    dt.title_royalty,
    (dt.title_ytd_sales * dt.title_royalty / 100) AS royalty_amount_per_title,
    t.royaltyper,
    (dt.title_ytd_sales * t.royaltyper / 100) AS royalty_amount_per_author,
    FROM stg_TitleAuthors t
LEFT JOIN dim_titles dt ON t.title_id = dt.title_id
LEFT JOIN dim_authors a ON t.author_id = a.author_id
LEFT JOIN dim_publishers p ON dt.publisher_id = p.publisher_id
left join dim_date d on dt.published_year= d.YEAR