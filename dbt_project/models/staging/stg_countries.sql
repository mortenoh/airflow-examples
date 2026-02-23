-- Staging model: clean and type-cast raw country data
select
    country_code,
    country_name,
    cast(population as bigint) as population,
    cast(area as double precision) as area_km2,
    region,
    subregion,
    loaded_at
from {{ source('raw', 'raw_countries') }}
where country_code is not null
