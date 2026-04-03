# automate-dv-kimball

A dbt package that generates **Kimball dimensional models** (star schemas) on top of **Data Vault 2.0** raw vaults built with [AutomateDV](https://github.com/Datavault-UK/automate-dv).

## What it does

Provides macros that transform DV2 hubs, links, and satellites into Kimball dimensions and facts — the consumption layer that business users and BI tools query.

## Supported platforms

Snowflake, BigQuery, PostgreSQL, Databricks, SQL Server

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/sathwikbs/automate-dv-kimball.git"
    revision: v1.0.0
```

Then run:

```bash
dbt deps
```

This automatically installs AutomateDV and dbt-utils as dependencies.

## Macros

### Dimensions

| Macro | Purpose |
|-------|---------|
| `automate_dv_kimball.dim()` | Dimension tables (SCD Type 1, 2, integer surrogate keys) |
| `automate_dv_kimball.dim_bridge()` | Kimball bridge tables (many-to-many resolution) |

### Facts

| Macro | Purpose |
|-------|---------|
| `automate_dv_kimball.fact()` | Transaction fact tables |
| `automate_dv_kimball.fact_periodic_snapshot()` | Periodic snapshot fact tables |
| `automate_dv_kimball.fact_accumulating_snapshot()` | Accumulating snapshot fact tables |
| `automate_dv_kimball.fact_factless()` | Factless fact tables (coverage/intersection) |

### Bus Matrix Helpers

| Macro | Purpose |
|-------|---------|
| `automate_dv_kimball.bus_matrix()` | Read bus matrix config from dbt_project.yml |
| `automate_dv_kimball.validate_star()` | Validate bus matrix config (run-operation) |
| `automate_dv_kimball.generate_star()` | Generate model scaffolds (run-operation) |

### Generic Tests

**Data Vault 2.0:**
`hub_pk_unique_not_null`, `hub_nk_not_null`, `link_fk_to_hub`, `sat_hashdiff_not_null`, `sat_pk_to_parent`, `eff_sat_no_overlap`, `eff_sat_no_gap`, `ma_sat_cdk_unique`, `pit_pk_as_of_unique`, `pit_covers_all_entities`, `xts_satellite_name_valid`

**Kimball:**
`dim_sk_unique_not_null`, `dim_scd2_no_gap`, `dim_scd2_one_current`, `fact_fk_to_dim`, `fact_measure_not_null`, `factless_no_duplicate`, `periodic_snapshot_no_dup_grain`, `accum_snapshot_milestones_ordered`

## Quick Start

```sql
-- models/dim_customer.sql
{{ config(materialized='table') }}

{{ automate_dv_kimball.dim(
    src_pk='CUSTOMER_HK',
    src_nk='CUSTOMER_ID',
    src_ldts='LOAD_DATETIME',
    source_model='hub_customer',
    satellites={
        'sat_customer_details': {
            'pk': 'CUSTOMER_HK',
            'ldts': 'LOAD_DATETIME',
            'payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE']
        }
    },
    scd_type=1) }}
```

```sql
-- models/fact_order.sql
{{ config(materialized='table') }}

{{ automate_dv_kimball.fact(
    src_pk='ORDER_HK',
    src_fk=['CUSTOMER_HK', 'PRODUCT_HK'],
    src_ldts='LOAD_DATETIME',
    source_model='link_order',
    dimensions={
        'CUSTOMER_HK': 'dim_customer',
        'PRODUCT_HK': 'dim_product'
    },
    satellites={
        'sat_order_details': {
            'pk': 'ORDER_HK',
            'ldts': 'LOAD_DATETIME',
            'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
        }
    }) }}
```

## DV2 to Kimball Mapping

| DV2 Source | Kimball Target | Macro |
|---|---|---|
| Hub + Sat(s) | Dimension (SCD1/SCD2) | `dim()` |
| Hub + MA-Sat | Pivoted dimension | Hand-built SQL |
| Hub + Eff-Sat | SCD2 dimension | Hand-built SQL |
| Ref Table | Static dimension | `dim()` or SELECT |
| Link + Sat(s) | Transaction fact | `fact()` |
| NH-Link | Transaction fact | Hand-built SQL |
| Link (no measures) | Factless fact | `fact_factless()` |
| Link + milestone Sats | Accumulating snapshot | `fact_accumulating_snapshot()` |
| Hub + periodic Sat | Periodic snapshot | `fact_periodic_snapshot()` |
| DV2 Bridge | Kimball bridge | `dim_bridge()` |

## Sample Project

For a complete working example (raw source through semantic layer), see [automate-dv-kimball-sample](https://github.com/sathwikbs/automate-dv-kimball-sample).

## AI Skill

The `skills/dv2-to-kimball/` directory contains an AI skill that teaches Cursor, Codex, and other AI-assisted IDEs how to correctly map DV2 assets to Kimball models.

## License

Apache License 2.0. This package depends on [AutomateDV](https://github.com/Datavault-UK/automate-dv) (Apache 2.0, Copyright Business Thinking Ltd.).
