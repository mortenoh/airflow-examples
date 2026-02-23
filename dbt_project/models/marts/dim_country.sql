-- Cleaned country dimension with density calculation
select
    country_code,
    country_name,
    population,
    area_km2,
    case
        when area_km2 > 0 then round(cast(population as numeric) / cast(area_km2 as numeric), 1)
        else 0
    end as population_density,
    region,
    subregion
from {{ ref('stg_countries') }}
