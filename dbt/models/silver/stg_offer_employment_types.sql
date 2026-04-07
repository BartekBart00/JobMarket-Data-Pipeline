{{ config(tags=["silver", "normalized"]) }}

select
    ro.id as offer_id,
    ro.slug,
    u.unnest."from" as salary_from,
    u.unnest.fromperunit as from_per_unit,
    u.unnest."to" as salary_to,
    u.unnest.toperunit as to_per_unit,
    u.unnest.currency as currency,
    u.unnest.currencysource as currency_source,
    u.unnest.type as compensation_type,
    u.unnest.unit as rate_unit,
    u.unnest.gross as is_gross
from {{ source("bronze", "raw_offers") }} as ro,
    unnest(ro."employmentTypes") as u
