{{ config(materialized='table', schema='gold') }}

WITH resolved_skills AS (
    SELECT 
        s.offer_id,
        s.skill_level,
        COALESCE(
            CASE 
                WHEN m.status = 'success' THEN m.clean_skill 
                ELSE m.raw_skill 
            END, 
            s.skill_name
        ) AS final_skill_name
    FROM {{ ref('stg_offer_required_skills') }} s
    LEFT JOIN {{ source('silver', 'ai_skill_mapping') }} m 
        ON LOWER(TRIM(s.skill_name)) = LOWER(TRIM(m.raw_skill))
),

offers AS (
    SELECT id AS offer_id, company_name 
    FROM {{ ref('stg_offers') }}
)

SELECT 
    md5(r.offer_id::VARCHAR) AS offer_key,
    md5(LOWER(TRIM(o.company_name))) AS company_key,
    md5(LOWER(TRIM(r.final_skill_name))) AS skill_key,
    
    r.skill_level
FROM resolved_skills r
LEFT JOIN offers o 
    ON r.offer_id = o.offer_id
WHERE r.final_skill_name IS NOT NULL