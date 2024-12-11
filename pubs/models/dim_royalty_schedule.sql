WITH stg_royshed AS (
    SELECT * 
    FROM {{ source('pubs', 'RoySched') }}