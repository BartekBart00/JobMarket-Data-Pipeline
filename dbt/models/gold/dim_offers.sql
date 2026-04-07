{{ config(materialized='table', schema='gold') }}

SELECT 
    md5(id::VARCHAR) AS offer_key, 
    id AS offer_id,       
    slug,
    title,
    experience_level,
    workplace_type,
    working_time,
    city,
    country_code
FROM {{ ref('stg_offers') }}