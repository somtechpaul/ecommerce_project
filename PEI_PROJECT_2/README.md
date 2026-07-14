# PEI Sales Data Pipeline

## Assignment Deliverables

| Requirement | Table / File |
|---|---|
| Raw customer | ecommerce_dev.bronze.customer |
| Raw products | ecommerce_dev.bronze.products |
| Raw orders | ecommerce_dev.bronze.orders |
| Customer enrichment | ecommerce_dev.silver.customer_enriched |
| Product enrichment | ecommerce_dev.silver.products_enriched |
| Enriched orders | ecommerce_dev.gold.sales_master |
| Required profit aggregate | ecommerce_dev.gold.profit_summary |
| SQL aggregates | sql/analytics/profit_aggregates.sql |

## Pipeline Flow

Landing → Bronze → Schema Validation → Data Quality →
Enrichment → Gold

## Tests

pytest tests/unit
pytest tests/integration