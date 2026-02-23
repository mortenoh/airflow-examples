# API Pipelines

## Weather & Climate APIs

DAGs 81--100 are where it all comes together. Instead of fake data, these pipelines call
**real public APIs** (no API keys needed) to fetch weather forecasts, earthquake data,
health statistics, and more. They combine everything from the earlier sections: `@task`
functions, pandas transforms, error handling, and multi-step orchestration.

DAGs 81--86 focus on weather and climate data. They use the `airflow_examples.apis` helper
module for consistent API access.

### Open-Meteo Ecosystem

[Open-Meteo](https://open-meteo.com/) provides a family of free weather APIs with no authentication
required. The project uses seven endpoints:

| Endpoint | Base URL | Data |
|----------|----------|------|
| Forecast | `api.open-meteo.com/v1/forecast` | Hourly/daily forecasts up to 16 days |
| Archive | `archive-api.open-meteo.com/v1/archive` | Historical weather data back to 1940 |
| Air Quality | `air-quality-api.open-meteo.com/v1/air-quality` | PM2.5, PM10, NO2, O3, European AQI |
| Marine | `marine-api.open-meteo.com/v1/marine` | Wave height, swell, wave period |
| Flood | `flood-api.open-meteo.com/v1/flood` | River discharge forecasts (GloFAS) |
| Geocoding | `geocoding-api.open-meteo.com/v1/search` | City name to coordinates resolution |
| Elevation | `api.open-meteo.com/v1/elevation` | Elevation for given coordinates |

All Open-Meteo calls go through `fetch_open_meteo()` which automatically adds `timezone=auto`.

### Dynamic Task Mapping with .expand()

DAG 81 demonstrates mapping a task across multiple cities using Airflow's dynamic task mapping:

```python
from airflow_examples.apis import NORDIC_CITIES

@task
def fetch_forecast(city: dict[str, object]) -> dict[str, object]:
    params = {"latitude": city["lat"], "longitude": city["lon"], ...}
    return fetch_open_meteo(OPEN_METEO_FORECAST, params)

# Creates 5 parallel task instances, one per city
fetched = fetch_forecast.expand(city=NORDIC_CITIES)
combined = combine_forecasts(fetched)  # receives list of all results
```

The `.expand()` call creates one task instance per item in the list at runtime. Downstream tasks
receiving the mapped output get a list of all results automatically.

### Forecast Verification Methodology

DAG 82 computes standard forecast accuracy metrics:

- **MAE (Mean Absolute Error)**: Average of |forecast - actual|. Interpretable in the same units
  as the variable (e.g., degrees Celsius).
- **RMSE (Root Mean Squared Error)**: Penalizes large errors more heavily than MAE. Always >= MAE.
- **Bias**: Mean of (forecast - actual). Positive = systematic over-prediction.
- **Lead-time degradation**: Accuracy typically worsens from day 1 to day 7. DAG 82 groups by
  forecast lead time to quantify this effect.

### AQI Classification and WHO Thresholds

DAG 83 classifies European AQI values using WHO 2021 air quality guidelines:

| AQI Range | Category | PM2.5 (ug/m3) | PM10 (ug/m3) | NO2 (ug/m3) |
|-----------|----------|---------------|--------------|-------------|
| 0--20 | Good | < 15 | < 45 | < 25 |
| 20--40 | Fair | 15--30 | 45--90 | 25--50 |
| 40--60 | Moderate | 30--55 | 90--180 | 50--100 |
| 60--80 | Poor | 55--110 | 180--360 | 100--200 |
| 80--100 | Very Poor | 110--220 | 360--720 | 200--400 |
| > 100 | Extremely Poor | > 220 | > 720 | > 400 |

The DAG identifies exceedance hours (values exceeding WHO guideline levels) and generates
per-city health advisories based on the worst observed category.

### Composite Risk Index

DAG 84 combines marine forecast data with river flood discharge into a weighted composite:

```
Marine Risk Score (0-100):    wave_height < 1m = 0, 1-2m = 25, 2-4m = 50, 4-6m = 75, > 6m = 100
Flood Risk Score (0-100):     discharge/mean < 1.5x = 0, 1.5-3x = 50, > 3x = 100
Composite = 0.6 * marine + 0.4 * flood
Categories: Low (0-25), Moderate (25-50), High (50-75), Extreme (75-100)
```

### Per-DAG Reference

| DAG | API | Key Concept |
|-----|-----|-------------|
| 81 | Open-Meteo Forecast | `.expand()` dynamic mapping, cross-city comparison |
| 82 | Forecast + Archive | MAE, RMSE, bias, lead-time degradation |
| 83 | Air Quality | WHO thresholds, AQI classification, health advisories |
| 84 | Marine + Flood | Composite risk scoring, weighted index |
| 85 | Sunrise-Sunset | Latitude-daylight correlation, seasonal variation |
| 86 | Geocoding + Elevation | Disambiguation, coordinate-driven enrichment |

---

## Geographic & Economic APIs

DAGs 87--90 work with country, economic, and cross-API data. They demonstrate handling pagination,
nested JSON, financial time series, and multi-source enrichment.

### REST Countries Nested JSON

The REST Countries API returns deeply nested structures:

```json
{
  "name": {"common": "Norway", "official": "Kingdom of Norway"},
  "languages": {"nno": "Norwegian Nynorsk", "nob": "Norwegian Bokmal", "smi": "Sami"},
  "currencies": {"NOK": {"name": "Norwegian krone", "symbol": "kr"}},
  "borders": ["FIN", "SWE", "RUS"],
  "latlng": [62.0, 10.0]
}
```

DAG 87 flattens this into relational tables:
- **Flat country table**: code, name, population, area, lat, lon
- **Bridge table (country_language)**: country, lang_code, language
- **Bridge table (country_currency)**: country, currency_code, currency_name
- **Edge list (borders)**: from, to (undirected graph of country borders)

### World Bank API Pagination

The World Bank API returns `[metadata, records]` per page. The `fetch_world_bank_paginated()`
helper iterates all pages automatically:

```python
# Response format: [{"page": 1, "pages": 3, "total": 150}, [records...]]
records = fetch_world_bank_paginated("NOR;SWE", "NY.GDP.PCAP.CD", "2000:2023")
```

DAG 88 fetches three indicators (GDP, CO2, renewable energy), joins them on country+year,
and computes Pearson correlations to explore relationships like GDP vs CO2 emissions.

### Frankfurter Currency Time Series

DAG 89 fetches a full year of EUR exchange rates and applies financial analysis:

- **Log returns**: `ln(price_t / price_{t-1})`, preferred over simple returns for statistical properties
- **Rolling volatility**: 30-day rolling standard deviation, annualized by multiplying by sqrt(252)
- **Correlation matrix**: how Nordic currencies co-move with EUR
- **Event detection**: dates with returns > 2 standard deviations from the mean

### Cross-API Enrichment Pattern

DAG 90 demonstrates the enrichment pattern: start with REST Countries base data, then use
coordinates from each country's capital to call Open-Meteo for weather and air quality. The
result is a unified profile combining static metadata with real-time environmental readings.

### Per-DAG Reference

| DAG | API | Key Concept |
|-----|-----|-------------|
| 87 | REST Countries | Nested JSON, bridge tables, border graph |
| 88 | World Bank | Pagination, multi-indicator join, Pearson correlation |
| 89 | Frankfurter | Log returns, rolling volatility, event detection |
| 90 | REST Countries + Open-Meteo | Cross-API enrichment, unified profiles |

---

## Geophysical & Environmental APIs

DAGs 91--94 cover earthquake analysis, carbon intensity, hypothesis testing, and long-term
climate trends.

### GeoJSON FeatureCollection

The USGS Earthquake API returns GeoJSON:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {"mag": 5.2, "place": "Pacific Ocean", "time": 1700000000000},
      "geometry": {"type": "Point", "coordinates": [-150.5, 60.1, 10.0]}
    }
  ]
}
```

The `fetch_usgs_earthquakes()` helper flattens this to flat dicts with lat, lon, depth,
magnitude, place, and time fields.

### Gutenberg-Richter Law

DAG 91 fits the Gutenberg-Richter frequency-magnitude relation:

```
log10(N) = a - b * M
```

Where N is the cumulative number of earthquakes with magnitude >= M. The **b-value** (typically
~1.0) describes the ratio of small to large earthquakes. Regional variations in b-value can
indicate different tectonic stress regimes.

### UK Carbon Intensity Data Model

DAG 92 uses the UK Carbon Intensity API which provides half-hourly data:

- **Intensity**: forecast and actual gCO2/kWh, with an index category
- **Generation mix**: percentage breakdown by fuel type (gas, wind, nuclear, solar, biomass, etc.)

The DAG identifies cleanest/dirtiest hours (useful for scheduling energy-intensive workloads),
compares weekday vs weekend patterns, and evaluates forecast accuracy.

### Null Hypothesis Testing

DAG 93 is a deliberate data science lesson. It fetches earthquakes and weather for Iceland,
computes correlations, and confirms that weather does not predict earthquakes (r ~ 0).

Key lessons:
1. Correlation does not imply causation
2. Testing obvious null hypotheses validates your statistical pipeline
3. Spurious correlations are common in large datasets (data dredging)
4. Always consider the physical mechanism before claiming a relationship

### Chunked Historical Requests

DAG 94 fetches 20 years of daily climate data in 4-year chunks to respect API limits:

```python
year_ranges = [(2004, 2007), (2008, 2011), (2012, 2015), (2016, 2019), (2020, 2024)]
for start_year, end_year in year_ranges:
    data = fetch_open_meteo(OPEN_METEO_ARCHIVE, {..., "start_date": f"{start_year}-01-01"})
```

After concatenation, the DAG computes:
- Linear regression temperature trend (degrees per decade)
- Monthly climatology (long-term mean per month)
- Anomalies from climatology (which years were unusually warm/cold)
- Extreme event frequency (days above 30C or below -20C)

### Per-DAG Reference

| DAG | API | Key Concept |
|-----|-----|-------------|
| 91 | USGS Earthquake | GeoJSON parsing, Gutenberg-Richter, spatial binning |
| 92 | UK Carbon Intensity | Generation mix, daily profiles, forecast accuracy |
| 93 | USGS + Open-Meteo | Null hypothesis, spurious correlation lesson |
| 94 | Open-Meteo Archive | Chunked requests, linear regression, seasonal decomposition |

---

## Global Health Indicators

DAGs 95--97 work with WHO and World Bank health data, demonstrating OData APIs,
cross-organization joins, and dimensional modeling.

### WHO GHO OData API

The WHO Global Health Observatory (GHO) provides indicators via an OData endpoint:

```
GET https://ghoapi.azureedge.net/api/WHOSIS_000001?$filter=SpatialDim eq 'NOR'&$top=100
```

Response fields include:
- `SpatialDim`: ISO3 country code
- `TimeDim`: year
- `Dim1`: sex (BTSX = both sexes, FMLE = female, MLE = male)
- `NumericValue`: the indicator value

The `fetch_who_indicator()` helper constructs the OData URL with `$filter` and `$top` parameters.

### Cross-Organization Data Joining

DAG 96 joins World Bank health spending with WHO infant mortality. The challenge is that both
organizations use ISO3 country codes but different API structures:

- **World Bank**: `{"country": {"id": "NOR"}, "date": "2023", "value": 85000}`
- **WHO**: `{"SpatialDim": "NOR", "TimeDim": 2023, "NumericValue": 1.8}`

The join key is (country_code, year), requiring field-level normalization before merging.

### Star Schema Design

DAG 97 builds a classic star schema from multiple API sources:

```
dim_country (country_key, code, name, region, population, area)
dim_time (time_key, year, decade, is_21st_century)
dim_indicator (indicator_key, code, name, source_org, unit, category)
fact_health_indicator (country_key, time_key, indicator_key, value)
```

Each dimension is populated from a different source (REST Countries, generated, manual
definition), and the fact table combines World Bank + WHO indicators with surrogate key
references. A composite health index normalizes each indicator to 0--100 and computes a
weighted average.

### Per-DAG Reference

| DAG | API | Key Concept |
|-----|-----|-------------|
| 95 | WHO GHO | OData API, gender gap analysis, life expectancy trends |
| 96 | World Bank + WHO | Cross-org join, log-linear regression, efficiency ranking |
| 97 | REST Countries + World Bank + WHO | Star schema, composite health index |

---

## Advanced Multi-API Data Engineering

DAGs 98--100 bring together all the patterns from previous sections into increasingly
sophisticated data engineering pipelines.

### Multi-API Orchestration with Staging Layers

DAG 98 orchestrates 6 different APIs into a single dashboard:

```
REST Countries ─┐
Open-Meteo     ─┤
Air Quality    ─┼─> Stage Raw ─> Integrate ─> Dashboard Metrics ─> Report
Frankfurter    ─┤
World Bank     ─┘
```

The staging layer writes each raw API response to Parquet files before integration. This
pattern provides:
- **Reproducibility**: raw data preserved for re-processing
- **Debugging**: inspect intermediate state at each layer
- **Idempotency**: staging is overwritten, integration is rebuilt from staging

### Quality Framework on Live Data

DAG 99 applies the `quality.py` framework (from DAGs 68--72) to live API responses:

- **Schema validation**: expected fields present in each API response
- **Completeness**: null rates within acceptable thresholds
- **Statistical bounds**: temperature within [-60, 60]C, precipitation >= 0, etc.
- **Cross-source integrity**: country codes from REST Countries exist in World Bank data

This demonstrates that quality checks are not just for batch files -- they apply equally to
real-time API data.

### SCD Type 2 Implementation

DAG 100 (the capstone) implements Slowly Changing Dimension Type 2:

```
Previous snapshot:  NOR | Norway | population=5400000 | valid_from=2024-01-01 | valid_to=9999-12-31
Current extract:    NOR | Norway | population=5450000

After SCD Type 2:
  NOR | Norway | pop=5400000 | valid_from=2024-01-01 | valid_to=2025-01-01 | is_current=False
  NOR | Norway | pop=5450000 | valid_from=2025-01-01 | valid_to=9999-12-31 | is_current=True
```

Key components:
- **Surrogate keys**: hash-based deterministic integer keys (not business keys)
- **Change detection**: compare current extract with previous snapshot
- **SCD rows**: old row gets `valid_to` set and `is_current=False`, new row inserted
- **Audit trail**: extraction timestamp, row counts, change counts, schema version
- **Validation**: referential integrity, no orphan keys, date range validity

### Per-DAG Reference

| DAG | APIs | Key Concept |
|-----|------|-------------|
| 98 | 6 APIs (REST Countries, Open-Meteo x2, Frankfurter, World Bank) | Staging layers, livability score |
| 99 | Open-Meteo + REST Countries + World Bank | Quality framework on live API data |
| 100 | REST Countries + World Bank | SCD Type 2, surrogate keys, audit trail (capstone) |
