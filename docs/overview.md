{% docs __overview__ %}

# AutomateDV

AutomateDV is a dbt package that generates Data Vault 2.0 and Kimball dimensional model SQL from metadata.

## Data Vault 2.0 Macros

| Macro | Purpose |
|-------|---------|
| `hub` | Hub tables (business entity identity) |
| `link` | Link tables (relationships between hubs) |
| `sat` | Satellite tables (descriptive attributes with history) |
| `eff_sat` | Effectivity satellites (relationship validity tracking) |
| `ma_sat` | Multi-active satellites (multiple concurrent records per key) |
| `t_link` | Transactional links (event-based relationships) |
| `nh_link` | Non-historized links (insert-only event records) |
| `pit` | Point-in-time tables (satellite timeline resolution) |
| `bridge` | Bridge tables (hub-to-hub traversal via links) |
| `xts` | Extended tracking satellites (satellite coverage tracking) |
| `ref_table` | Reference tables (static lookup data) |
| `stage` | Staging views (hashing, derived columns, ranking) |

## Kimball Dimensional Macros

| Macro | Purpose |
|-------|---------|
| `dim` | Dimension tables (SCD Type 1, 2, integer surrogate keys) |
| `fact` | Transaction fact tables |
| `fact_periodic_snapshot` | Periodic snapshot fact tables |
| `fact_accumulating_snapshot` | Accumulating snapshot fact tables |
| `fact_factless` | Factless fact tables (coverage/intersection) |
| `dim_bridge` | Kimball bridge tables (many-to-many resolution) |

## Generic Tests

AutomateDV ships reusable generic tests for both DV2 and Kimball models.

### Data Vault 2.0 Tests

| Test | Purpose | Applies To |
|------|---------|------------|
| `hub_pk_unique_not_null` | Hub PK is unique and not null | Hubs |
| `hub_nk_not_null` | Natural key is never null | Hubs |
| `link_fk_to_hub` | Link FK resolves to a hub PK | Links |
| `sat_hashdiff_not_null` | Hashdiff is never null | Satellites |
| `sat_pk_to_parent` | Satellite PK resolves to parent hub/link | Satellites |
| `eff_sat_no_overlap` | No overlapping date ranges per entity | Eff-Sats |
| `eff_sat_no_gap` | No gaps in date ranges per entity | Eff-Sats |
| `ma_sat_cdk_unique` | PK + CDK + LDTS combination is unique | MA-Sats |
| `pit_pk_as_of_unique` | One PIT record per entity per as-of date | PIT |
| `pit_covers_all_entities` | PIT covers every entity in the hub | PIT |
| `xts_satellite_name_valid` | Satellite names match a known list | XTS |

### Kimball Dimensional Tests

| Test | Purpose | Applies To |
|------|---------|------------|
| `dim_sk_unique_not_null` | Surrogate key is unique and not null | Dimensions |
| `dim_scd2_no_gap` | No gaps in SCD2 validity windows | SCD2 Dims |
| `dim_scd2_one_current` | Exactly one current record per NK | SCD2 Dims |
| `fact_fk_to_dim` | Fact FK resolves to a dimension PK | Facts |
| `fact_measure_not_null` | Measure column is never null | Facts |
| `factless_no_duplicate` | No duplicate grain rows (single PK column) | Factless Facts |
| `periodic_snapshot_no_dup_grain` | No duplicate grain rows (composite) | Periodic Snapshots |
| `accum_snapshot_milestones_ordered` | Milestone dates in chronological order | Accum Snapshots |

## Semantic Layer (MetricFlow)

AutomateDV includes ready-made MetricFlow semantic models and metrics that sit on top of the Kimball dimensional models. Requires dbt >= 1.6 and `dbt-metricflow`.

### Semantic Models

| Model | Source | Entities | Measures |
|-------|--------|----------|----------|
| `sem_customers` | `dim_customer` | customer (primary) | -- |
| `sem_products` | `dim_product` | product (primary) | unit_price |
| `sem_dates` | `dim_date` | date_key (primary) | -- |
| `sem_orders` | `fact_order` | order_id, customer, product, date_key | order_amount, order_quantity, order_count |
| `sem_order_fulfillment` | `fact_order_fulfillment` | order_id, customer | fulfillment_order_amount, order_to_ship_days, fulfillment_count |
| `sem_inventory` | `fact_inventory_daily` | product | quantity_on_hand (semi-additive) |
| `sem_payments` | `fact_payment` | payment, customer | payment_amount, payment_count |
| `sem_promotions` | `fact_promotion_coverage` | promo, customer, product | promotion_count |

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `total_revenue` | simple | Sum of order amounts |
| `total_orders` | simple | Count of orders |
| `average_order_value` | derived | Revenue per order |
| `total_quantity_sold` | simple | Total items sold |
| `revenue_per_unit` | derived | Revenue per unit |
| `total_payments` | simple | Sum of payments |
| `total_payment_count` | simple | Count of payments |
| `avg_fulfillment_days` | simple | Avg order-to-ship days |
| `total_fulfillment_orders` | simple | Count of fulfilled orders |
| `current_inventory` | simple | Latest inventory on hand |
| `promotion_coverage_count` | simple | Count of promotions |

## Supported Platforms

Snowflake, BigQuery, PostgreSQL, Databricks, SQL Server

For full documentation visit [automate-dv.readthedocs.io](https://automate-dv.readthedocs.io).

{% enddocs %}
