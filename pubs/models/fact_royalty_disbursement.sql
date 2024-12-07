WITH stg_titles AS (
    SELECT
        t.titles_key,
        t.publisher_id AS publishers_key,
        t.published_date,
        SUM(t.title_ytd_sales) AS ytd_sales,
        SUM(t.title_ytd_sales * (t.title_royalty / 100)) AS royalty_amount_per_title
    FROM {{ ref('dim_titles') }} t
    GROUP BY t.titles_key, t.publisher_id, t.published_date
),
stg_titleauthor AS (
    SELECT
        ta.title_id AS titles_key,
        CONCAT(ta.title_id, '_', ta.au_id) AS author_title_key, 
        ta.royaltyper AS author_royalty_rate
    FROM {{ source('pubs', 'TitleAuthor') }} ta
),
stg_royalties_by_author AS (
    SELECT
        t.titles_key,
        ta.author_title_key,
        SUM(t.title_ytd_sales) AS ytd_sales,
        SUM(t.title_ytd_sales * (ta.author_royalty_rate / 100)) AS royalty_amount_per_author
    FROM {{ ref('dim_titles') }} t
    INNER JOIN stg_titleauthor ta
        ON t.titles_key = ta.titles_key
    GROUP BY t.titles_key, ta.author_title_key
),
stg_dates AS (
    SELECT
        datekey,
        date
    FROM {{ ref('dim_date') }}
)
SELECT
    rt.titles_key,
    ra.author_title_key,
    d.datekey as date_key,
    rt.publishers_key,
    rt.published_date,
    rt.ytd_sales,
    rt.royalty_amount_per_title,
    ra.royalty_amount_per_author
FROM stg_titles rt
LEFT JOIN stg_royalties_by_author ra
    ON rt.titles_key = ra.titles_key
INNER JOIN stg_dates d
    ON d.date = rt.published_date