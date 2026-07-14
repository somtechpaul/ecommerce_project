# Sales Data Processing Pipeline

## Overview

This project implements a metadata-driven sales data processing pipeline using
Databricks, PySpark, Delta Lake, and Unity Catalog.

The pipeline ingests customer, product, and order datasets, validates and cleans
the data, applies business enrichment, builds an order-level Sales Master, and
creates the requested profitability aggregate tables.

All transformation and table-building logic is implemented using PySpark.
SQL is used only for the four analytical outputs explicitly requested in the
assignment.




## Assignment Requirement Mapping

| Requirement | Implementation |
|---|---|
| Raw customer table | `ecommerce_dev.bronze.customer` |
| Raw product table | `ecommerce_dev.bronze.products` |
| Raw order table | `ecommerce_dev.bronze.orders` |
| Customer enriched table | `ecommerce_dev.silver.customer_enriched` |
| Product enriched table | `ecommerce_dev.silver.products_enriched` |
| Enriched order table | `ecommerce_dev.gold.sales_master` |
| Profit rounded to two decimals | `profit` column in `gold.sales_master` |
| Customer name and country | Columns in `gold.sales_master` |
| Product category and subcategory | `product_category` and `product_sub_category` |
| Profit by year/category/subcategory/customer | `ecommerce_dev.gold.profit_summary` |
| Required SQL outputs | `sql/analytics/profit_aggregates.sql` |


## Raw Tables

The Bronze ingestion pipeline creates one Delta table for each source:

- `ecommerce_dev.bronze.customer`
- `ecommerce_dev.bronze.products`
- `ecommerce_dev.bronze.orders`

Source column names are standardized, and operational metadata is added:

- `pipeline_run_id`
- `source_file_name`
- `source_system`
- `ingestion_timestamp`
- `ingestion_date`

Business columns are stored as strings in Bronze to preserve source values.
Datatype enforcement is performed later using metadata-driven tolerant casting.

## Schema Validation

Expected schemas are stored in metadata and applied dynamically.

The schema validation stage:

1. Reads the Bronze table.
2. Retrieves the expected schema from metadata.
3. Safely casts source columns using tolerant casting.
4. Separates datatype-invalid records.
5. Enforces required fields.
6. Validates missing columns and datatype mismatches.
7. Writes valid data to the validated Silver layer.
8. Logs rejected records and pipeline metrics.

Invalid values are preserved for investigation rather than causing the complete
pipeline to fail.

## Data Quality

The data quality layer applies configurable business rules such as:

- `NOT_NULL`
- `UNIQUE`
- `POSITIVE`

Records that fail quality checks are written to configured invalid tables and
the central rejected-record audit table. Valid records continue to enrichment
and Gold processing.

## Customer Enrichment

Customer enrichment is implemented in:

`src/pei_pipeline/transformations/enrichment.py`

The enriched customer table adds:

- `customer_full_address`
- `customer_region`

The full address combines address, city, state, and country. Region is
standardized to uppercase.

## Product Enrichment

Product enrichment is implemented in:

`src/pei_pipeline/transformations/enrichment.py`

The enriched product table adds:

- `category_name`
- `sub_category_name`

These values are standardized using title-case formatting.

## Sales Master

The Sales Master is built in:

`src/pei_pipeline/transformations/gold.py`

It left-joins validated orders with enriched customer and product tables.

The table includes:

### Order Information

- Order ID
- Order date
- Ship date
- Ship mode
- Quantity
- Price
- Discount
- Profit

### Customer Information

- Customer ID
- Customer name
- Country
- Segment
- City
- State
- Region
- Full address

### Product Information

- Product ID
- Product name
- Product category
- Product subcategory
- Product price

Profit is rounded to two decimal places while constructing the Sales Master.

Left joins are used so every order remains in the master table even when
customer or product reference data is missing.

## Required Profit Aggregate

The required aggregate is stored as:

`ecommerce_dev.gold.profit_summary`

The table groups profit by:

- Order year
- Product category
- Product subcategory
- Customer ID
- Customer name

The output columns are:

- `order_year`
- `product_category`
- `product_sub_category`
- `customer_id`
- `customer_name`
- `total_profit`

Customer ID is retained alongside customer name so different customers with
the same name are not combined.

## SQL Outputs

The requested SQL queries are stored in:

`sql/analytics/profit_aggregates.sql`

The file contains:

1. Profit by Year
2. Profit by Year and Product Category
3. Profit by Customer
4. Profit by Customer and Year

## Testing Strategy

The project follows a layered testing approach.

### Unit Tests

Located under:

`tests/unit`

The tests cover:

- File readers
- Filesystem utilities
- Column mapping
- Safe datatype casting
- Schema validation
- Data quality rules
- Customer and product enrichment
- Gold Sales Master
- Profit Summary
- Table readers and writers

### Integration Tests

Located under:

`tests/integration`

The tests validate orchestration for:

- Bronze ingestion
- Schema validation
- Full pipeline stage execution

### End-to-End Validation

Located under:

`tests/e2e`

Smoke validation checks that required Bronze, Silver, enriched, Gold, and audit
tables exist and contain expected columns and records.

## Error Handling

The pipeline uses stage-level and file-level exception handling.

Failures capture:

- Pipeline stage
- Source
- Source file
- Source and target table
- Records read
- Records written
- Rejected records
- Error message
- Start and end timestamps

Rejected records are preserved with the failed column, expected datatype,
actual value, rejection reason, and raw record.

## Scalability Considerations

- Processing logic uses distributed Spark DataFrames.
- Metadata is collected only because configuration datasets are small.
- Delta Lake is used for reliable table storage.
- Table writes support append, overwrite, and merge modes.
- Pipelines are separated into independent stages that can run as individual
  Databricks Workflow tasks.
- Reusable transformation modules keep notebooks thin and support packaging as
  a Python wheel.
- Repeated row-count actions are retained for assignment visibility but would be
  minimized in a production-scale implementation.
- Source-specific partitioning and Auto Loader checkpoints would be considered
  for high-volume continuous ingestion.

## Assumptions

- The source datasets are batch files.
- Customer ID and Product ID are the reference keys used for enrichment.
- Profit from the source order dataset is the authoritative profit measure.
- Order date is available as a valid date after schema validation.
- Customer and product enrichment tables contain one row per business key.
- SQL queries consume the Gold Sales Master.

## Known Production Enhancements

For a larger production implementation, the following enhancements would be
considered:

- Auto Loader with checkpointing for scalable file discovery
- Processed-file ledger for complete ingestion idempotency
- Metadata-driven source-specific partitioning
- Stable row identifiers for rule-level rejection tracking
- Additional DQ rules such as regex, domain, and range validation
- Decimal financial datatypes instead of floating-point types
- Databricks Declarative Automation Bundles for automated deployment
- CI/CD execution of unit and integration tests
