{{ config(tags=["silver", "core"]) }}

with latest_unified_title as (
    select
        offer_id,
        unified_title,
        row_number() over (
            partition by offer_id
            order by processed_at desc
        ) as rn
    from {{ source("bronze", "raw_offer_unified_title") }}
    where status = 'success'
)

select
    ro.id,
    ro.slug,
    lu.unified_title as title,
    ro."experienceLevel" as experience_level,
    ro."companyName" as company_name,
    ro.city,
    ro."countryCode" as country_code,
    ro."companySize" as company_size,
    ro."workplaceType" as workplace_type,
    ro."workingTime" as working_time
from {{ source("bronze", "raw_offers") }} as ro
left join latest_unified_title as lu
    on ro.id = lu.offer_id
    and lu.rn = 1
