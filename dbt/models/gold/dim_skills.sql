{{ config(materialized='table', schema='gold') }}

WITH all_skills AS (
    SELECT clean_skill AS skill_name
    FROM {{ source('silver', 'ai_skill_mapping') }}
    WHERE status = 'success' AND clean_skill IS NOT NULL
    
    UNION 
    
    SELECT s.skill_name AS skill_name
    FROM {{ ref('stg_offer_required_skills') }} s
    LEFT JOIN {{ source('silver', 'ai_skill_mapping') }} m 
        ON LOWER(TRIM(s.skill_name)) = LOWER(TRIM(m.raw_skill))
    WHERE m.raw_skill IS NULL OR m.status = 'error'
)

SELECT 
    md5(LOWER(TRIM(skill_name))) AS skill_key,
    MAX(skill_name) AS skill_name
FROM all_skills
WHERE skill_name IS NOT NULL
GROUP BY LOWER(TRIM(skill_name))