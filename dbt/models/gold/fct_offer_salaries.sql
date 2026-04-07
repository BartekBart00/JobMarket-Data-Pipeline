{{ config(materialized='table', schema='gold') }}

WITH salaries AS (
    SELECT * FROM {{ ref('stg_offer_employment_types') }}
    WHERE currency_source = 'original'
),

offers AS (
    SELECT id AS offer_id, company_name 
    FROM {{ ref('stg_offers') }}
)

SELECT 
    md5(s.offer_id::VARCHAR) AS offer_key,
    md5(LOWER(TRIM(o.company_name))) AS company_key,
    
    s.compensation_type,
    s.is_gross,
    s.salary_from,
    s.salary_to,
    (s.salary_from + s.salary_to) / 2.0 AS salary_avg,
    s.currency
FROM salaries s
LEFT JOIN offers o 
    ON s.offer_id = o.offer_id