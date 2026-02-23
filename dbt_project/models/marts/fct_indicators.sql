-- Fact table joining indicators with country dimension
select
    i.country_code,
    c.country_name,
    i.year,
    i.indicator,
    i.value,
    c.region,
    c.subregion
from {{ ref('stg_indicators') }} i
left join {{ ref('dim_country') }} c
    on i.country_code = c.country_code
