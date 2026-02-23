"""DAG 87: REST Countries Demographics Analysis.

Fetches country data from the REST Countries API, parses nested JSON
structures (languages, currencies, borders), builds relational tables,
and produces a demographic comparison of Nordic countries.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/87_demographics")


@task
def fetch_nordic_countries() -> list[dict[str, object]]:
    """Fetch Nordic countries from REST Countries by alpha codes."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/alpha?codes=NOR,SWE,DNK,FIN,ISL"
    params = {"fields": "name,cca3,population,area,languages,currencies,borders,latlng,gini"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []
    print(f"[{timestamp()}] Fetched {len(countries)} Nordic countries")
    return countries


@task
def fetch_all_european() -> list[dict[str, object]]:
    """Fetch all European countries for broader comparison."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/region/europe"
    params = {"fields": "name,cca3,population,area"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []
    print(f"[{timestamp()}] Fetched {len(countries)} European countries")
    return countries


@task
def parse_nested_structures(countries: list[dict[str, object]]) -> dict[str, object]:
    """Extract nested JSON into flat columns and bridge tables."""
    flat_countries: list[dict[str, object]] = []
    country_languages: list[dict[str, str]] = []
    country_currencies: list[dict[str, str]] = []

    for c in countries:
        name_obj: dict[str, object] = c.get("name", {})  # type: ignore[assignment]
        cca3 = str(c.get("cca3", ""))
        latlng: list[object] = c.get("latlng", [0, 0])  # type: ignore[assignment]

        flat_countries.append({
            "code": cca3,
            "name": name_obj.get("common", ""),
            "population": c.get("population"),
            "area": c.get("area"),
            "lat": latlng[0] if len(latlng) > 0 else None,
            "lon": latlng[1] if len(latlng) > 1 else None,
            "borders": c.get("borders", []),
        })

        languages: dict[str, str] = c.get("languages", {})  # type: ignore[assignment]
        for code, name in languages.items():
            country_languages.append({"country": cca3, "lang_code": code, "language": name})

        currencies: dict[str, dict[str, str]] = c.get("currencies", {})  # type: ignore[assignment]
        for code, info in currencies.items():
            country_currencies.append({
                "country": cca3,
                "currency_code": code,
                "currency_name": info.get("name", ""),
            })

    print(f"[{timestamp()}] Parsed {len(flat_countries)} countries, "
          f"{len(country_languages)} language links, {len(country_currencies)} currency links")
    return {
        "countries": flat_countries,
        "country_languages": country_languages,
        "country_currencies": country_currencies,
    }


@task
def build_comparison(
    parsed: dict[str, object],
    european: list[dict[str, object]],
) -> dict[str, object]:
    """Rank Nordic countries by population, area, density, and borders."""
    countries: list[dict[str, object]] = parsed.get("countries", [])  # type: ignore[assignment]

    ranked: list[dict[str, object]] = []
    for c in countries:
        pop = c.get("population", 0)
        area = c.get("area", 1)
        density = float(str(pop)) / float(str(area)) if float(str(area)) > 0 else 0
        borders: list[str] = c.get("borders", [])  # type: ignore[assignment]
        ranked.append({
            "name": c["name"],
            "population": pop,
            "area": area,
            "density": round(density, 1),
            "num_borders": len(borders),
        })

    ranked.sort(key=lambda x: float(str(x["population"])), reverse=True)
    print(f"[{timestamp()}] Nordic ranking by population:")
    for i, r in enumerate(ranked, 1):
        print(f"  {i}. {r['name']}: pop={r['population']}, area={r['area']}km2, "
              f"density={r['density']}/km2, borders={r['num_borders']}")

    eu_count = len(european)
    return {"ranked": ranked, "european_total": eu_count}


@task
def build_border_graph(parsed: dict[str, object]) -> dict[str, object]:
    """Create edge list of country borders and identify components."""
    countries: list[dict[str, object]] = parsed.get("countries", [])  # type: ignore[assignment]

    edges: list[dict[str, str]] = []
    for c in countries:
        code = str(c.get("code", ""))
        borders: list[str] = c.get("borders", [])  # type: ignore[assignment]
        for neighbor in borders:
            edge = {"from": min(code, neighbor), "to": max(code, neighbor)}
            if edge not in edges:
                edges.append(edge)

    nodes = {str(c.get("code", "")) for c in countries}
    for e in edges:
        nodes.add(e["from"])
        nodes.add(e["to"])

    print(f"[{timestamp()}] Border graph: {len(nodes)} nodes, {len(edges)} edges")
    for e in edges:
        print(f"  {e['from']} -- {e['to']}")

    return {"nodes": list(nodes), "edges": edges}


@task
def report(
    parsed: dict[str, object],
    comparison: dict[str, object],
    border_graph: dict[str, object],
) -> None:
    """Print demographics table, bridge tables, and border graph."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[{timestamp()}] === Country Demographics Report ===")

    ranked: list[dict[str, object]] = comparison.get("ranked", [])  # type: ignore[assignment]
    print("\n  Demographics:")
    print(f"  {'Name':<15} {'Pop':>12} {'Area(km2)':>12} {'Density':>8} {'Borders':>8}")
    for r in ranked:
        print(f"  {str(r['name']):<15} {r['population']:>12} {r['area']:>12} "
              f"{r['density']:>8} {r['num_borders']:>8}")

    langs: list[dict[str, str]] = parsed.get("country_languages", [])  # type: ignore[assignment]
    print(f"\n  Language Bridge Table ({len(langs)} rows):")
    for row in langs:
        print(f"    {row['country']}: {row['language']}")

    currs: list[dict[str, str]] = parsed.get("country_currencies", [])  # type: ignore[assignment]
    print(f"\n  Currency Bridge Table ({len(currs)} rows):")
    for row in currs:
        print(f"    {row['country']}: {row['currency_code']} ({row['currency_name']})")

    edges: list[dict[str, str]] = border_graph.get("edges", [])  # type: ignore[assignment]
    print(f"\n  Border Graph ({len(edges)} edges):")
    for e in edges:
        print(f"    {e['from']} -- {e['to']}")


with DAG(
    dag_id="087_country_demographics",
    default_args=DEFAULT_ARGS,
    description="REST Countries demographics with nested JSON parsing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    nordic = fetch_nordic_countries()
    european = fetch_all_european()
    parsed = parse_nested_structures(countries=nordic)
    comparison = build_comparison(parsed=parsed, european=european)
    borders = build_border_graph(parsed=parsed)
    report(parsed=parsed, comparison=comparison, border_graph=borders)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )
