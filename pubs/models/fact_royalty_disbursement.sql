WITH 
stg_titles AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} AS titles_key,
        title_id,
        title,
        pub_id,
        price ,
        ytd_sales,
        royalty 
    FROM {{ source('pubs', 'Titles') }}
),
stg_authors AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['AU_ID']) }} AS authors_key,    
        AU_ID AS author_id,                                            
        CONCAT(au_fname, ' ', au_lname) AS author_name,                                   
        phone AS author_phone,                                            
        address AS author_address,                                        
        city AS author_city,                                              
        state AS author_state,                                            
        zip AS author_zip                                            
    FROM {{ source('pubs', 'Authors') }}
),
stg_publishers AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['pub_id']) }} AS publishers_key,
        pub_id,
        pub_name AS publisher_name
    FROM {{ source('pubs', 'Publishers') }}
),

stg_date AS (
    SELECT 
        ord_date,
        TO_CHAR(ord_date, 'YYYYMMDD') AS orderdate_key,
        EXTRACT(YEAR FROM ord_date) AS order_year
    FROM {{ source('pubs', 'Sales') }}
),
stg_TitleAuthors AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['au_id']) }} AS authors_key, 
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} AS titles_key, 
        title_id,
        au_id,
        AU_ORD, 
        royaltyper                               
    FROM {{ source('pubs', 'TitleAuthor') }}
),
stg_sales AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ord_num', 'title_id', 'stor_id']) }} AS order_number, 
        ord_num,
        ord_date,
        TO_CHAR(ord_date, 'YYYYMMDD') AS orderdate_key,
        EXTRACT(YEAR FROM ord_date) AS order_year,
        title_id,
        stor_id,
        qty
    FROM {{ source('pubs', 'Sales') }}
)
SELECT 
    t.titles_key,
    a.authors_key,
    publishers_key,
    s.order_year AS YEAR,
    t.ytd_sales,
    t.royalty AS Percentage_royalty,
    (t.ytd_sales * t.royalty / 100) AS total_royalty,
    ta.royaltyper AS AUTHORS_PERCENTAGE,
    (t.ytd_sales * t.royalty / 100 * ta.royaltyper / 100) AS royalty_per_author
FROM 
    stg_TitleAuthors ta
LEFT JOIN 
    stg_titles t ON ta.titles_key = t.titles_key
LEFT JOIN 
    stg_authors a ON ta.authors_key = a.authors_key
LEFT JOIN
    stg_sales s ON t.title_id = s.title_id
LEFT JOIN 
    stg_publishers p ON t.pub_id = p.pub_id 
ORDER BY 
    s.order_year