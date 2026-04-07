{{ config(materialized='table', schema='gold') }}

WITH normalized_companies AS (
    SELECT 
        LOWER(TRIM(company_name)) AS normalized_name,
        MAX(company_name) AS display_name,
        MAX(company_size) AS company_size
    FROM {{ ref('stg_offers') }}
    WHERE company_name IS NOT NULL
    GROUP BY LOWER(TRIM(company_name))
)

SELECT 
    md5(normalized_name) AS company_key,
    display_name AS company_name,
    company_size,
    CASE company_size
        WHEN '1-10' THEN 1
        WHEN '11-25' THEN 2
        WHEN '26-50' THEN 3
        WHEN '51-100' THEN 4
        WHEN '101-500' THEN 5
        WHEN '501+' THEN 6
        ELSE 99 
    END AS company_size_sort_idx
FROM normalized_companies