-- Staging model: clean and type-cast raw indicator data
select
    country_code,
    cast(year as integer) as year,
    indicator,
    cast(value as double precision) as value,
    loaded_at
from {{ source('raw', 'raw_indicators') }}
where country_code is not null
  and value is not null
