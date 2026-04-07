{{ config(tags=["silver", "normalized"]) }}

select
    ro.id as offer_id,
    ro.slug,
    u.unnest.name as skill_name,
    u.unnest.level as skill_level
from {{ source("bronze", "raw_offers") }} as ro,
    unnest(ro."requiredSkills") as u
