# Semantic Layer (MetricFlow)

This directory contains MetricFlow semantic models and metrics definitions
that sit on top of the Kimball dimensional models.

## Requirements

- **dbt Core >= 1.6** (semantic_models YAML key not supported in earlier versions)
- **dbt-metricflow** installed: `pip install "dbt-metricflow[your-adapter]"`

## Setup

Add this directory to your `model-paths` in `dbt_project.yml`:

```yaml
model-paths: ["models", "semantic_layer"]
```

Then run:

```bash
dbt parse                    # validate semantic models and metrics
dbt sl list metrics          # list all defined metrics
dbt sl list dimensions --metrics total_revenue  # show dimensions for a metric
```

## Files

| File | Contents |
|------|----------|
| `sem_dimensions.yml` | Semantic models for dim_customer, dim_product, dim_date |
| `sem_facts.yml` | Semantic models for fact_order, fact_order_fulfillment, fact_inventory_daily, fact_payment, fact_promotion_coverage |
| `sem_metrics.yml` | Business metrics: total_revenue, average_order_value, avg_fulfillment_days, current_inventory, etc. |

## Architecture

```
Raw Source -> Staging -> Raw Vault -> Business Vault -> Kimball Star Schema -> Semantic Layer -> BI Tools
```

The semantic layer is the final consumption layer. It defines:
- **Entities**: join keys (hash keys from DV2 flow through to Kimball FKs)
- **Dimensions**: categorical and time attributes for slicing/dicing
- **Measures**: aggregatable numeric values
- **Metrics**: business KPIs composed from measures (simple, derived, ratio)
