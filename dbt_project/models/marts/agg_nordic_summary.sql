-- Nordic country summary: latest GDP per capita and population
select
    c.country_code,
    c.country_name,
    c.population,
    c.area_km2,
    c.population_density,
    gdp.value as gdp_per_capita,
    gdp.year as gdp_year
from {{ ref('dim_country') }} c
left join (
    select
        country_code,
        value,
        year,
        row_number() over (partition by country_code order by year desc) as rn
    from {{ ref('fct_indicators') }}
    where indicator = 'gdp_per_capita'
) gdp
    on c.country_code = gdp.country_code
    and gdp.rn = 1
where c.subregion = 'Northern Europe'
order by gdp.value desc nulls last
