# Post-Generation Validation Rules

After generating a Kimball model, check every rule below. Fix any violation before considering the model complete.

## Structural Checks

### S1: Hub existence for every link FK

Every FK hash key in a link must have a corresponding hub.

```
link_order has CUSTOMER_HK  -->  hub_customer must exist
link_order has PRODUCT_HK   -->  hub_product must exist
nh_link_payment has ORDER_HK --> hub_order must exist
```

**How to check:** For each `src_fk` column in a fact's `source_model`, grep raw_vault/ for a hub whose `src_pk` matches.

### S2: Dimension exists for every fact FK

Every FK in a fact's `dimensions` dict must reference a dimension model that exists.

```yaml
dimensions:
  CUSTOMER_HK: dim_customer    # dim_customer.sql must exist
  PRODUCT_HK: dim_product      # dim_product.sql must exist
```

**How to check:** Verify each dimension name in the dict resolves via `ref()`.

### S3: Satellite columns exist

Every column listed in `satellites.measures` or `satellites.payload` must actually exist in the satellite model's output.

**How to check:** Read the satellite model and verify the column names match.

### S4: src_fk columns live on the source model

The `src_fk` list must only contain columns that physically exist on the `source_model` (the link). Columns from satellites must NOT be in `src_fk` — they come through the `satellites` dict instead.

```
GOOD: src_fk = ['CUSTOMER_HK', 'PRODUCT_HK']     # both on link_order
BAD:  src_fk = ['CUSTOMER_HK', 'ORDER_DATE_KEY']  # ORDER_DATE_KEY is on a satellite
```

### S5: No duplicate columns across satellites

If a fact joins multiple satellites, no two satellites should provide the same column name. This causes a `COLUMN_ALREADY_EXISTS` error at runtime.

## Layer Discipline Checks

### L1: No raw_source references from Kimball

Kimball models must NEVER use `ref('raw_xxx')`. The flow is:

```
raw_source -> staging -> raw_vault -> (BV) -> kimball
```

### L2: No staging references from Kimball

Kimball models must NEVER use `ref('stg_xxx')`.

### L3: Business vault reads only from raw vault

BV models must only reference raw_vault models (hubs, links, sats), never staging or raw_source.

### L4: Dimensions before facts

Every fact must declare `-- depends_on: {{ ref('dim_xxx') }}` for each dimension it joins. This ensures dbt builds dimensions first. The `automate_dv.fact()` macro does this automatically via the `dimensions` dict. For hand-built facts, add the comment manually.

## DV2 Standards Checks

### D1: Links have no context columns

Links should contain ONLY: hash key (PK), FK hash keys, LOAD_DATETIME, SOURCE. No descriptive attributes, no date keys, no business columns. Those belong in satellites.

### D2: BV models have SOURCE = 'SYSTEM'

Every business vault model must include `'SYSTEM' AS SOURCE` to indicate system-generated origin per DV2 standards.

### D3: SCD2 dimensions require hashdiff

If `scd_type=2`, every satellite in the `satellites` dict must include `'hashdiff': 'HASHDIFF'`. Without it, change detection is unreliable.

### D4: Effectivity dates come from source

Eff-sat `START_DATE` and `END_DATE` must originate from the source system, not be system-generated. Verify the staging model passes these through from raw_source.

## Kimball Standards Checks

### K1: Fact grain is clear

Every fact must have a single, unambiguous grain:
- Transaction fact: one row per event (ORDER_HK = one order)
- Periodic snapshot: one row per entity per period (PRODUCT_HK + snapshot_date)
- Accumulating snapshot: one row per lifecycle instance (ORDER_HK)
- Factless: one row per intersection (PROMO_COV_HK)

### K2: Semi-additive measures annotated

Periodic snapshot facts with balance/level measures (inventory, account balance) must mark them as `semi_additive` in the satellite config. These measures should use latest-snapshot-only aggregation, not SUM across time.

### K3: Conformed dimensions consistent

If a dimension is used across multiple facts, the FK column name should be the same everywhere. E.g., `CUSTOMER_HK` in every fact that joins `dim_customer`, not `CUST_HK` in one and `CUSTOMER_HK` in another.

### K4: Role-playing dimensions declared

When a fact joins the same dimension multiple times (e.g., `dim_date` for order_date AND ship_date), use the role-playing syntax:

```yaml
dimensions:
  ORDER_DATE_KEY: {'dim': 'dim_date', 'role': 'order_date'}
  SHIP_DATE_KEY: {'dim': 'dim_date', 'role': 'ship_date'}
```

### K5: Lag fact columns allow NULLs

Accumulating snapshot lag facts (e.g., ORDER_TO_SHIP_DAYS) can legitimately be NULL when a milestone hasn't been reached yet. Do NOT add `not_null` tests to lag fact columns.

## Schema Test Checks

### T1: Dimension PK tests

Every dimension should have these tests on its primary key:

```yaml
columns:
  - name: CUSTOMER_HK
    tests:
      - not_null
      - unique
      - automate_dv.dim_sk_unique_not_null
```

### T2: SCD2 dimension tests

SCD2 dimensions should additionally have:

```yaml
columns:
  - name: CUSTOMER_HK    # or the NK column
    tests:
      - automate_dv.dim_scd2_no_gap
      - automate_dv.dim_scd2_one_current
```

### T3: Fact FK-to-dimension tests

Every FK in a fact should verify referential integrity:

```yaml
columns:
  - name: CUSTOMER_HK
    tests:
      - automate_dv.fact_fk_to_dim:
          dim_model: ref('dim_customer')
          dim_pk: CUSTOMER_HK
```

### T4: Fact measure tests

Fact measures should be tested for not-null (except lag facts in accumulating snapshots):

```yaml
columns:
  - name: ORDER_AMOUNT
    tests:
      - not_null
      - automate_dv.fact_measure_not_null
```

### T5: Factless fact uniqueness

Factless facts should verify no duplicate grain rows:

```yaml
columns:
  - name: PROMO_COV_HK
    tests:
      - automate_dv.factless_no_duplicate
```

## Quick Validation Checklist

Use this after generating any model:

```
[ ] S1: Every link FK has a hub
[ ] S2: Every fact dimension reference exists
[ ] S3: Satellite column names are correct
[ ] S4: src_fk only has columns from the source model
[ ] S5: No duplicate columns across satellites
[ ] L1: No raw_source refs from Kimball
[ ] L2: No staging refs from Kimball
[ ] L4: Dimensions load before facts
[ ] D1: Links have no context columns
[ ] K1: Fact grain is clear and documented
[ ] T1-T5: Schema tests added
```
