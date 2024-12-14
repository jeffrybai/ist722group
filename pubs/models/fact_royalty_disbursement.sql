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

dim_date AS (
    SELECT 
    TO_CHAR(DATE, 'YYYYMMDD') AS DATEKEY,
        DATE,
        YEAR,
        MONTH,
        QUARTER,
        DAY, 
        DAYOFWEEK,
        WEEKOFYEAR,
        DAYOFYEAR,
        QUARTERNAME,
        MONTHNAME,
        DAYNAME,
        WEEKDAY
    FROM {{ ref('dim_date') }}
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
    d.DATEKEY AS date_key,
    s.order_year ,
    t.ytd_sales,
    t.royalty AS royalty_percentage,
    round(t.ytd_sales * t.royalty / 100,2) AS total_royalty,
    ta.royaltyper AS authors_percentage,
   round(t.ytd_sales* t.royalty / 100 * ta.royaltyper / 100,2) AS royalty_per_author
FROM 
    stg_TitleAuthors ta
LEFT JOIN 
    stg_titles t ON ta.titles_key = t.titles_key
LEFT JOIN 
    stg_authors a ON ta.authors_key = a.authors_key
LEFT JOIN
    stg_sales s ON t.title_id = s.title_id
LEFT JOIN 
 dim_date d ON s.orderdate_key = d.DATEKEY