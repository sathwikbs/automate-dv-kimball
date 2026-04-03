---
name: dv2-to-kimball
description: Guide developers building Kimball dimensional models on top of Data Vault 2.0 raw vault. Use when the user asks to create a dimension, fact table, star schema, or map DV2 assets to Kimball, or mentions automate_dv dim/fact macros.
---

# DV2-to-Kimball Mapping Advisor

You are helping a developer build Kimball dimensional models that consume Data Vault 2.0 assets via the `automate_dv` dbt package.

## Workflow

When a developer asks to build a Kimball model:

1. Ask: "What business question should this model answer?"
2. Identify the DV2 assets involved (read raw_vault/ and business_vault/ models)
3. Look up the mapping rule in the decision table below
4. Generate the model using the correct macro and parameters
5. Validate against the rules in [validation-rules.md](validation-rules.md)
6. Add schema tests in the appropriate `schema.yml`

## Decision Table: DV2 Assets to Kimball Models

| DV2 Source | Kimball Target | Macro | When to Use |
|---|---|---|---|
| Hub + Sat(s) | Dimension (SCD1) | `automate_dv.dim(scd_type=1)` | One current row per entity, latest attributes |
| Hub + Sat(s) + hashdiff | Dimension (SCD2) | `automate_dv.dim(scd_type=2)` | Full history with EFFECTIVE_FROM/TO windows |
| Hub + Sat(s) | Dimension (integer SK) | `automate_dv.dim(surrogate_key='integer')` | When downstream tools need integer keys |
| Hub + MA-Sat | Dimension (pivoted) | Hand-built SQL | Pivot multi-active rows into one wide row per entity |
| Hub + Eff-Sat | Dimension (SCD2) | Hand-built SQL | Relationship validity windows become EFFECTIVE_FROM/TO |
| Hub + PIT + Sat(s) | Dimension (point-in-time) | Hand-built SQL | PIT resolves correct satellite versions for multi-sat joins |
| Hub + XTS | Dimension (audit) | Hand-built SQL | Track satellite coverage per entity |
| Ref Table | Dimension (static) | `SELECT *` or `automate_dv.dim()` | Code/lookup tables (countries, statuses) |
| BV Computed Sat | Dimension (enriched) | `automate_dv.dim()` | Hub + raw sat + BV-derived attributes |
| BV Deduped Hub | Dimension (resolved) | `automate_dv.dim()` | Same-as-link resolved entity as dim source |
| Link + Sat(s) | Transaction Fact | `automate_dv.fact()` | Link = grain, satellite measures = fact columns |
| NH-Link | Transaction Fact | Hand-built SQL | Insert-only events; payload = measures |
| Link (no measures) | Factless Fact | `automate_dv.fact_factless()` | Relationship existence IS the fact |
| Link + milestone Sats | Accumulating Snapshot | `automate_dv.fact_accumulating_snapshot()` | Track lifecycle with milestone dates + lag facts |
| Hub + periodic Sat | Periodic Snapshot | `automate_dv.fact_periodic_snapshot()` | Regular snapshots (daily inventory, monthly balance) |
| DV2 Bridge/Link | Kimball Bridge | `automate_dv.dim_bridge()` | Many-to-many relationship resolution |

## Macro Signatures

### automate_dv.dim()

```jinja
{{ automate_dv.dim(
    src_pk='CUSTOMER_HK',
    src_nk='CUSTOMER_ID',
    src_ldts='LOAD_DATETIME',
    source_model='hub_customer',
    satellites={
        'sat_customer_details': {
            'pk': 'CUSTOMER_HK',
            'ldts': 'LOAD_DATETIME',
            'payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE'],
            'hashdiff': 'HASHDIFF'       -- required for SCD2
        }
    },
    scd_type=1) }}
```

- `source_model`: always a hub (or BV-resolved hub)
- `satellites`: dict of satellite models with pk, ldts, payload columns
- `hashdiff`: required for SCD2, omit for SCD1
- `scd_type`: 1 (latest row) or 2 (full history with EFFECTIVE_FROM/TO/IS_CURRENT)

### automate_dv.fact()

```jinja
{{ automate_dv.fact(
    src_pk='ORDER_HK',
    src_fk=['CUSTOMER_HK', 'PRODUCT_HK'],
    src_ldts='LOAD_DATETIME',
    source_model='link_order',
    dimensions={
        'CUSTOMER_HK': 'dim_customer',
        'PRODUCT_HK': 'dim_product',
        'ORDER_DATE_KEY': {'dim': 'dim_date', 'role': 'order_date'}
    },
    satellites={
        'sat_order_details': {
            'pk': 'ORDER_HK',
            'ldts': 'LOAD_DATETIME',
            'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
        }
    },
    degenerate_dimensions=[]) }}
```

- `source_model`: always a link
- `src_fk`: FK columns on the link (must exist on the link itself)
- `dimensions`: maps FK columns to dimension models; use `{'dim': ..., 'role': ...}` for role-playing dims
- `satellites`: dict of satellite models providing measures
- `degenerate_dimensions`: columns from the source model included as-is (no FK join)

### automate_dv.fact_accumulating_snapshot()

```jinja
{{ automate_dv.fact_accumulating_snapshot(
    src_pk='ORDER_HK',
    src_fk=['CUSTOMER_HK'],
    source_model='link_order',
    dimensions={'CUSTOMER_HK': 'dim_customer'},
    milestones={
        'ORDER_DATE': {
            'model': 'sat_order_placed',
            'pk': 'ORDER_HK',
            'date_col': 'LOAD_DATETIME',
            'dim': 'dim_date'
        },
        'SHIP_DATE': {
            'model': 'sat_order_shipped',
            'pk': 'ORDER_HK',
            'date_col': 'LOAD_DATETIME',
            'dim': 'dim_date'
        }
    },
    lag_facts={
        'ORDER_TO_SHIP_DAYS': {'from': 'ORDER_DATE', 'to': 'SHIP_DATE'}
    },
    satellites={...}) }}
```

- Each milestone comes from a **separate satellite** (one sat per lifecycle stage)
- `lag_facts`: computed DATEDIFF columns between milestones (can be NULL if milestone not yet reached)

### automate_dv.fact_periodic_snapshot()

```jinja
{{ automate_dv.fact_periodic_snapshot(
    src_pk='PRODUCT_HK',
    src_fk=[],
    src_ldts='LOAD_DATETIME',
    source_model='hub_product',
    dimensions={'PRODUCT_HK': 'dim_product'},
    satellites={
        'sat_inventory_level': {
            'pk': 'PRODUCT_HK',
            'ldts': 'LOAD_DATETIME',
            'measures': ['QUANTITY_ON_HAND'],
            'semi_additive': ['QUANTITY_ON_HAND']
        }
    },
    snapshot_period='day') }}
```

- `source_model`: a hub (grain is entity, not relationship)
- `semi_additive`: marks measures that should NOT be summed across time
- `snapshot_period`: 'day', 'week', 'month'

### automate_dv.fact_factless()

```jinja
{{ automate_dv.fact_factless(
    src_pk='PROMO_COV_HK',
    src_fk=['PRODUCT_HK', 'CUSTOMER_HK'],
    src_ldts='LOAD_DATETIME',
    source_model='link_promotion_coverage',
    dimensions={
        'PRODUCT_HK': 'dim_product',
        'CUSTOMER_HK': 'dim_customer'
    },
    include_count=true) }}
```

- No satellites needed: the link intersection IS the fact
- `include_count=true`: adds a RECORD_COUNT column

### automate_dv.dim_bridge()

```jinja
{{ automate_dv.dim_bridge(
    src_pk='CUST_ACCT_HK',
    bridge_fks=['CUSTOMER_HK', 'ACCOUNT_HK'],
    source_model='bridge_cust_acct',
    src_ldts='LOAD_DATETIME',
    weighting_factor='OWNERSHIP_PCT') }}
```

- Resolves many-to-many relationships for Kimball star schemas
- `weighting_factor`: optional allocation column

## Anti-Patterns (warn the developer)

1. **Descriptive attributes on a link**: columns like ORDER_NUMBER or ORDER_DATE belong in a satellite, not `src_extra_columns` on the link. The book says: "links must not contain time or context information except load date."

2. **Skipping layers**: Kimball models must NOT reference raw_source or staging directly. The flow is always: raw_source -> staging -> raw_vault -> (optional BV) -> kimball.

3. **Missing hub for link FK**: every FK hash key in a link must trace back to a hub. If you see `ACCOUNT_HK` as an FK but no `hub_account`, flag it.

4. **Facts without dimension FK enforcement**: every fact should have `-- depends_on: {{ ref('dim_xxx') }}` for each dimension to ensure dims load first.

5. **SCD2 without hashdiff**: if `scd_type=2`, the satellite config MUST include `'hashdiff': 'HASHDIFF'` for change detection.

6. **BV models without SOURCE = 'SYSTEM'**: business vault computed models must indicate system-generated origin per DV2 standards.

7. **Measures from the link**: fact measures come from **satellites**, not the link. The link only provides the grain (PK) and FK references.

## Layer Discipline

```
raw_source/    ->  staging/    ->  raw_vault/    ->  business_vault/  ->  test_kimball/
(synthetic)        (views)         (hubs,links,      (computed sats,      (dims, facts,
                                    sats,eff_sats,    same-as links,       bridges)
                                    ma_sats,nh_links,  milestones)
                                    pit,bridge,xts,
                                    ref_table)
```

- Staging reads ONLY from raw_source
- Raw vault reads ONLY from staging
- Business vault reads ONLY from raw vault
- Kimball reads from raw vault, business vault, and other Kimball models (dims referenced by facts)

## For worked examples, see [examples.md](examples.md)
## For post-generation validation, see [validation-rules.md](validation-rules.md)
