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

stg_TitleAuthors as (select
    {{ dbt_utils.generate_surrogate_key(['au_id']) }} as authors_key, 
    {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titles_key, 
    AU_ORD, 
    royaltyper ,                                             
FROM {{ source('pubs', 'TitleAuthor')}}
)

SELECT 
    dt.titles_key,
    a.authors_key,
    p.publishers_key,
    dt.title_id,
    a.author_id,
    dt.title_title as title,
    p.publisher_name,
    dt.published_year,
    a.author_name,
    dt.title_ytd_sales,
    dt.title_royalty,
    (dt.title_ytd_sales * dt.title_royalty / 100) AS royalty_amount_per_title,
    t.royaltyper,
    (dt.title_ytd_sales * t.royaltyper / 100) AS royalty_amount_per_author,
    FROM stg_TitleAuthors t
LEFT JOIN dim_titles dt ON t.titles_key = dt.titles_key
LEFT JOIN dim_authors a ON t.authors_key = a.authors_key
LEFT JOIN dim_publishers p ON dt.publishers_key = p.publishers_key
left join dim_date d on dt.published_year= d.YEAR