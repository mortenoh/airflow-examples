.PHONY: help install dev build export clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## Install dependencies
	npm install

dev: install ## Start dev server with hot reload
	npx slidev slides.md

build: install ## Build static site to dist/
	npx slidev build slides.md

export: install ## Export slides to PDF
	npx playwright install chromium
	npx slidev export slides.md --output airflow-slides.pdf

clean: ## Remove dist/ and node_modules/
	rm -rf dist node_modules
