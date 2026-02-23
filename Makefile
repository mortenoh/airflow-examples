.PHONY: help sync lint fmt test clean cli docs docs-build dashboard run-dbt start run stop

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# -- Build / Lint / Format / Test ------------------------------------------

sync: ## Install/sync dependencies
	uv sync

lint: ## Run ruff check and mypy
	uv run ruff check
	uv run mypy src/

fmt: ## Format with ruff
	uv run ruff format
	uv run ruff check --fix

test: ## Run pytest (no Docker needed, uses SQLite backend)
	uv run pytest tests/

clean: ## Remove .venv and caches
	rm -rf .venv __pycache__ .mypy_cache .ruff_cache .pytest_cache

# -- CLI --------------------------------------------------------------------

cli: ## Show Airflow REST API CLI help
	uv run af --help

# -- Docs -------------------------------------------------------------------

docs: ## Serve docs locally
	uv run mkdocs serve

docs-build: ## Build static docs site
	uv run mkdocs build

# -- Dashboard / dbt --------------------------------------------------------

dashboard: ## Launch Streamlit dashboard
	uv run streamlit run dashboard/app.py

run-dbt: ## Run dbt models against Postgres (requires running services)
	uv run dbt run --project-dir dbt_project --profiles-dir dbt_project
	uv run dbt test --project-dir dbt_project --profiles-dir dbt_project

# -- Run / Stop -------------------------------------------------------------

SCHEDULER = airflow-scheduler
TEST_DATE = 2025-01-01

start: ## Start Airflow (foreground) — Ctrl+C to stop
	@trap 'echo "\n\033[1;32mStopping Airflow...\033[0m"; docker compose down -v' EXIT; \
	docker compose up -d --wait; \
	echo "\033[1;32mAirflow is running\033[0m"; \
	echo "Web UI:  http://localhost:8081"; \
	echo "Mailpit: http://localhost:8025"; \
	echo "RustFS:  http://localhost:9001"; \
	echo "Grafana: http://localhost:3000"; \
	echo "Press Ctrl+C to stop Airflow"; \
	docker compose logs -f || true

run: ## Start Airflow (foreground), run all 108 DAGs, then serve web UI
	@echo "\033[1;32mStarting PostgreSQL + Airflow...\033[0m"
	docker compose up -d --wait
	@trap 'echo "\n\033[1;32mStopping Airflow...\033[0m"; docker compose down -v' EXIT; \
	echo "\033[1;32mWaiting for scheduler to parse DAGs...\033[0m"; \
	sleep 15; \
	echo "\n\033[1;34m-- 001_hello_world --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 001_hello_world $(TEST_DATE); \
	echo "\n\033[1;34m-- 002_python_operator --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 002_python_operator $(TEST_DATE); \
	echo "\n\033[1;34m-- 003_task_dependencies --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 003_task_dependencies $(TEST_DATE); \
	echo "\n\033[1;34m-- 004_taskflow_api --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 004_taskflow_api $(TEST_DATE); \
	echo "\n\033[1;34m-- 005_xcoms --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 005_xcoms $(TEST_DATE); \
	echo "\n\033[1;34m-- 006_branching --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 006_branching $(TEST_DATE); \
	echo "\n\033[1;34m-- 007_trigger_rules --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 007_trigger_rules $(TEST_DATE); \
	echo "\n\033[1;34m-- 008_templating --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 008_templating $(TEST_DATE); \
	echo "\n\033[1;34m-- 009_task_groups --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 009_task_groups $(TEST_DATE); \
	echo "\n\033[1;34m-- 010_dynamic_tasks --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 010_dynamic_tasks $(TEST_DATE); \
	echo "\n\033[1;34m-- 011_sensors --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 011_sensors $(TEST_DATE); \
	echo "\n\033[1;34m-- 012_retries_and_callbacks --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 012_retries_and_callbacks $(TEST_DATE); \
	echo "\n\033[1;34m-- 013_custom_operators --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 013_custom_operators $(TEST_DATE); \
	echo "\n\033[1;34m-- 014_assets (producer then consumer) --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 014_assets_producer $(TEST_DATE); \
	docker compose exec -T $(SCHEDULER) airflow dags test 014_assets_consumer $(TEST_DATE); \
	echo "\n\033[1;34m-- 015_dag_dependencies --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 015_dag_dependencies $(TEST_DATE); \
	echo "\n\033[1;34m-- 016_pools_and_priority --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 016_pools_and_priority $(TEST_DATE); \
	echo "\n\033[1;34m-- 017_variables_and_params --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 017_variables_and_params $(TEST_DATE); \
	echo "\n\033[1;34m-- 018_short_circuit --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 018_short_circuit $(TEST_DATE); \
	echo "\n\033[1;34m-- 019_setup_teardown --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 019_setup_teardown $(TEST_DATE); \
	echo "\n\033[1;34m-- 020_complex_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 020_complex_pipeline $(TEST_DATE); \
	echo "\n\033[1;35m-- Docker DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 021_docker_hello --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 021_docker_hello $(TEST_DATE); \
	echo "\n\033[1;34m-- 022_docker_python_script --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 022_docker_python_script $(TEST_DATE); \
	echo "\n\033[1;34m-- 023_docker_volumes --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 023_docker_volumes $(TEST_DATE); \
	echo "\n\033[1;34m-- 024_docker_env_and_secrets --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 024_docker_env_and_secrets $(TEST_DATE); \
	echo "\n\033[1;34m-- 025_docker_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 025_docker_pipeline $(TEST_DATE); \
	echo "\n\033[1;34m-- 026_docker_custom_image --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 026_docker_custom_image $(TEST_DATE); \
	echo "\n\033[1;34m-- 027_docker_network --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 027_docker_network $(TEST_DATE); \
	echo "\n\033[1;34m-- 028_docker_resources --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 028_docker_resources $(TEST_DATE); \
	echo "\n\033[1;34m-- 029_docker_xcom --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 029_docker_xcom $(TEST_DATE); \
	echo "\n\033[1;34m-- 030_docker_mixed_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 030_docker_mixed_pipeline $(TEST_DATE); \
	echo "\n\033[1;34m-- 031_docker_dynamic_images --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 031_docker_dynamic_images $(TEST_DATE); \
	echo "\n\033[1;34m-- 032_docker_compose_task --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 032_docker_compose_task $(TEST_DATE); \
	echo "\n\033[1;35m-- HTTP / SQL / SSH / Email / Deferrable DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 033_http_requests --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 033_http_requests $(TEST_DATE); \
	echo "\n\033[1;34m-- 034_http_sensor --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 034_http_sensor $(TEST_DATE); \
	echo "\n\033[1;34m-- 035_postgres_queries --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 035_postgres_queries $(TEST_DATE); \
	echo "\n\033[1;34m-- 036_sql_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 036_sql_pipeline $(TEST_DATE); \
	echo "\n\033[1;34m-- 037_generic_transfer --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 037_generic_transfer $(TEST_DATE); \
	echo "\n\033[1;34m-- 038_email_notifications --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 038_email_notifications $(TEST_DATE); \
	echo "\n\033[1;34m-- 039_ssh_commands --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 039_ssh_commands $(TEST_DATE); \
	echo "\n\033[1;34m-- 040_latest_only --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 040_latest_only $(TEST_DATE); \
	echo "\n\033[1;34m-- 041_deferrable_sensors --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 041_deferrable_sensors $(TEST_DATE); \
	echo "\n\033[1;34m-- 042_custom_deferrable --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 042_custom_deferrable $(TEST_DATE); \
	echo "\n\033[1;35m-- Bash DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 043_bash_basics --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 043_bash_basics $(TEST_DATE); \
	echo "\n\033[1;34m-- 044_bash_environment --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 044_bash_environment $(TEST_DATE); \
	echo "\n\033[1;34m-- 045_bash_scripting --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 045_bash_scripting $(TEST_DATE); \
	echo "\n\033[1;34m-- 046_bash_templating --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 046_bash_templating $(TEST_DATE); \
	echo "\n\033[1;35m-- Advanced Feature DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 047_taskflow_decorators --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 047_taskflow_decorators $(TEST_DATE); \
	echo "\n\033[1;34m-- 048_virtualenv_tasks --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 048_virtualenv_tasks $(TEST_DATE); \
	echo "\n\033[1;34m-- 049_empty_and_branch_operators --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 049_empty_and_branch_operators $(TEST_DATE); \
	echo "\n\033[1;34m-- 050_file_sensor --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 050_file_sensor $(TEST_DATE); \
	echo "\n\033[1;34m-- 051_advanced_retries --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 051_advanced_retries $(TEST_DATE); \
	echo "\n\033[1;34m-- 052_scheduling_features --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 052_scheduling_features $(TEST_DATE); \
	echo "\n\033[1;34m-- 053_custom_timetable --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 053_custom_timetable $(TEST_DATE); \
	echo "\n\033[1;34m-- 054_advanced_dynamic_mapping --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 054_advanced_dynamic_mapping $(TEST_DATE); \
	echo "\n\033[1;34m-- 055_multiple_assets (producers then consumers) --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 055_assets_temp_producer $(TEST_DATE); \
	docker compose exec -T $(SCHEDULER) airflow dags test 055_assets_humidity_producer $(TEST_DATE); \
	docker compose exec -T $(SCHEDULER) airflow dags test 055_assets_bulk_producer $(TEST_DATE); \
	docker compose exec -T $(SCHEDULER) airflow dags test 055_assets_dual_consumer $(TEST_DATE); \
	docker compose exec -T $(SCHEDULER) airflow dags test 055_assets_full_consumer $(TEST_DATE); \
	echo "\n\033[1;34m-- 056_custom_hook_and_sensor --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 056_custom_hook_and_sensor $(TEST_DATE); \
	echo "\n\033[1;34m-- 057_parquet_aggregation --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 057_parquet_aggregation $(TEST_DATE); \
	echo "\n\033[1;35m-- DHIS2 Metadata Pipeline DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 058_dhis2_org_units --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 058_dhis2_org_units $(TEST_DATE); \
	echo "\n\033[1;34m-- 059_dhis2_data_elements --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 059_dhis2_data_elements $(TEST_DATE); \
	echo "\n\033[1;34m-- 060_dhis2_indicators --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 060_dhis2_indicators $(TEST_DATE); \
	echo "\n\033[1;34m-- 061_dhis2_org_unit_geometry --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 061_dhis2_org_unit_geometry $(TEST_DATE); \
	echo "\n\033[1;34m-- 062_dhis2_combined_export --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 062_dhis2_combined_export $(TEST_DATE); \
	echo "\n\033[1;35m-- File-Based ETL Pipeline DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 063_csv_landing_zone --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 063_csv_landing_zone $(TEST_DATE); \
	echo "\n\033[1;34m-- 064_json_event_ingestion --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 064_json_event_ingestion $(TEST_DATE); \
	echo "\n\033[1;34m-- 065_multi_file_batch --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 065_multi_file_batch $(TEST_DATE); \
	echo "\n\033[1;34m-- 066_error_handling_etl --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 066_error_handling_etl $(TEST_DATE); \
	echo "\n\033[1;34m-- 067_incremental_file_processing --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 067_incremental_file_processing $(TEST_DATE); \
	echo "\n\033[1;35m-- Data Quality & Validation DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 068_schema_validation --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 068_schema_validation $(TEST_DATE); \
	echo "\n\033[1;34m-- 069_statistical_checks --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 069_statistical_checks $(TEST_DATE); \
	echo "\n\033[1;34m-- 070_freshness_completeness --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 070_freshness_completeness $(TEST_DATE); \
	echo "\n\033[1;34m-- 071_cross_dataset_validation --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 071_cross_dataset_validation $(TEST_DATE); \
	echo "\n\033[1;34m-- 072_quality_report --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 072_quality_report $(TEST_DATE); \
	echo "\n\033[1;35m-- Alerting & SLA Monitoring DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 073_sla_monitoring --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 073_sla_monitoring $(TEST_DATE); \
	echo "\n\033[1;34m-- 074_webhook_notifications --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 074_webhook_notifications $(TEST_DATE); \
	echo "\n\033[1;34m-- 075_failure_escalation --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 075_failure_escalation $(TEST_DATE); \
	echo "\n\033[1;34m-- 076_pipeline_health_check --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 076_pipeline_health_check $(TEST_DATE); \
	echo "\n\033[1;35m-- DAG Testing Pattern DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 077_testable_dag_pattern --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 077_testable_dag_pattern $(TEST_DATE); \
	echo "\n\033[1;34m-- 078_mock_api_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 078_mock_api_pipeline $(TEST_DATE); \
	echo "\n\033[1;34m-- 079_dag_test_runner --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 079_dag_test_runner $(TEST_DATE); \
	echo "\n\033[1;34m-- 080_parameterized_pipeline --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 080_parameterized_pipeline $(TEST_DATE); \
	echo "\n\033[1;35m-- Weather & Climate API DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 081_multi_city_forecast --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 081_multi_city_forecast $(TEST_DATE); \
	echo "\n\033[1;34m-- 082_forecast_vs_historical --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 082_forecast_vs_historical $(TEST_DATE); \
	echo "\n\033[1;34m-- 083_air_quality_monitoring --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 083_air_quality_monitoring $(TEST_DATE); \
	echo "\n\033[1;34m-- 084_marine_flood_risk --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 084_marine_flood_risk $(TEST_DATE); \
	echo "\n\033[1;34m-- 085_daylight_analysis --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 085_daylight_analysis $(TEST_DATE); \
	echo "\n\033[1;34m-- 086_geocoding_weather --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 086_geocoding_weather $(TEST_DATE); \
	echo "\n\033[1;35m-- Geographic & Economic API DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 087_country_demographics --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 087_country_demographics $(TEST_DATE); \
	echo "\n\033[1;34m-- 088_world_bank_indicators --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 088_world_bank_indicators $(TEST_DATE); \
	echo "\n\033[1;34m-- 089_currency_analysis --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 089_currency_analysis $(TEST_DATE); \
	echo "\n\033[1;34m-- 090_country_weather_enrichment --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 090_country_weather_enrichment $(TEST_DATE); \
	echo "\n\033[1;35m-- Geophysical & Environmental API DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 091_earthquake_analysis --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 091_earthquake_analysis $(TEST_DATE); \
	echo "\n\033[1;34m-- 092_carbon_intensity --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 092_carbon_intensity $(TEST_DATE); \
	echo "\n\033[1;34m-- 093_earthquake_weather_correlation --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 093_earthquake_weather_correlation $(TEST_DATE); \
	echo "\n\033[1;34m-- 094_climate_trends --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 094_climate_trends $(TEST_DATE); \
	echo "\n\033[1;35m-- Global Health Indicator DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 095_life_expectancy --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 095_life_expectancy $(TEST_DATE); \
	echo "\n\033[1;34m-- 096_health_expenditure --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 096_health_expenditure $(TEST_DATE); \
	echo "\n\033[1;34m-- 097_health_profile_star --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 097_health_profile_star $(TEST_DATE); \
	echo "\n\033[1;35m-- Advanced Multi-API DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 098_multi_api_dashboard --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 098_multi_api_dashboard $(TEST_DATE); \
	echo "\n\033[1;34m-- 099_api_quality_framework --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 099_api_quality_framework $(TEST_DATE); \
	echo "\n\033[1;34m-- 100_full_etl_scd --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 100_full_etl_scd $(TEST_DATE); \
	echo "\n\033[1;35m-- dbt Integration DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 101_dbt_load_raw --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 101_dbt_load_raw $(TEST_DATE); \
	echo "\n\033[1;34m-- 102_dbt_transform --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 102_dbt_transform $(TEST_DATE); \
	echo "\n\033[1;35m-- S3 Object Storage DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 103_minio_write --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 103_minio_write $(TEST_DATE); \
	echo "\n\033[1;34m-- 104_minio_data_lake --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 104_minio_data_lake $(TEST_DATE); \
	echo "\n\033[1;34m-- 105_object_storage --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 105_object_storage $(TEST_DATE); \
	echo "\n\033[1;35m-- Human-in-the-Loop DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 106_human_in_the_loop --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 106_human_in_the_loop $(TEST_DATE); \
	echo "\n\033[1;35m-- Variable-Driven Scheduling DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 107_variable_driven_scheduling --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 107_variable_driven_scheduling $(TEST_DATE); \
	echo "\n\033[1;35m-- Backfill Awareness DAGs --\033[0m"; \
	echo "\n\033[1;34m-- 108_backfill_awareness --\033[0m"; \
	docker compose exec -T $(SCHEDULER) airflow dags test 108_backfill_awareness $(TEST_DATE); \
	echo "\n\033[1;32mAll 108 airflow examples complete\033[0m"; \
	echo "Web UI: http://localhost:8081"; \
	echo "Mailpit: http://localhost:8025"; \
	echo "RustFS: http://localhost:9001"; \
	echo "Grafana: http://localhost:3000"; \
	echo "Press Ctrl+C to stop Airflow"; \
	docker compose logs -f || true

stop: ## Stop Airflow services and remove volumes
	docker compose down -v
