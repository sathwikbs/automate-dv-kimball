# Macro Reference

Complete parameter documentation for all automate-dv-kimball macros.

---

## Dimension Macros

### dim()

Generates a Kimball dimension table from a DV2 hub and one or more satellites.

```sql
{{ automate_dv_kimball.dim(
    src_pk,
    src_nk,
    src_ldts,
    source_model,
    satellites,
    scd_type,
    src_extra_columns,
    integer_surrogate) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Hub hash key column (e.g., `'CUSTOMER_HK'`) |
| `src_nk` | string | Yes | -- | Natural/business key column (e.g., `'CUSTOMER_ID'`) |
| `src_ldts` | string | Yes | -- | Load datetime column (e.g., `'LOAD_DATETIME'`) |
| `source_model` | string | Yes | -- | Hub model name (e.g., `'hub_customer'`) or BV-resolved hub |
| `satellites` | dict | Yes | -- | Satellite configurations (see below) |
| `scd_type` | int | No | 1 | `1` = latest row per entity. `2` = full history with EFFECTIVE_FROM/TO/IS_CURRENT |
| `src_extra_columns` | list | No | none | Additional columns from the hub to include |
| `integer_surrogate` | bool | No | false | If true, adds a `DIM_SK` integer surrogate key via DENSE_RANK |

**Satellite config structure:**

```yaml
satellites:
  sat_customer_details:       # satellite model name
    pk: CUSTOMER_HK           # satellite's parent hash key column
    ldts: LOAD_DATETIME        # satellite's load datetime column
    payload:                   # descriptive columns to include
      - CUSTOMER_NAME
      - CUSTOMER_PHONE
    hashdiff: HASHDIFF         # required for SCD2, omit for SCD1
```

**SCD1 example** — one current row per customer:

```sql
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

**SCD2 example** — full history with validity windows:

```sql
{{ automate_dv_kimball.dim(
    src_pk='PRODUCT_HK',
    src_nk='PRODUCT_ID',
    src_ldts='LOAD_DATETIME',
    source_model='hub_product',
    satellites={
        'sat_product_details': {
            'pk': 'PRODUCT_HK',
            'ldts': 'LOAD_DATETIME',
            'payload': ['PRODUCT_NAME', 'UNIT_PRICE'],
            'hashdiff': 'HASHDIFF'
        }
    },
    scd_type=2) }}
```

Output columns for SCD2: all payload columns plus `EFFECTIVE_FROM`, `EFFECTIVE_TO`, `IS_CURRENT`.

**Integer surrogate key example:**

```sql
{{ automate_dv_kimball.dim(
    src_pk='CUSTOMER_HK',
    src_nk='CUSTOMER_ID',
    src_ldts='LOAD_DATETIME',
    source_model='hub_customer',
    satellites={...},
    scd_type=1,
    integer_surrogate=true) }}
```

Adds `DIM_SK` (integer) as the first column via DENSE_RANK.

**Multi-satellite example** — flatten two satellites into one wide dimension:

```sql
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
        },
        'sat_customer_address': {
            'pk': 'CUSTOMER_HK',
            'ldts': 'LOAD_DATETIME',
            'payload': ['ADDRESS_LINE', 'CITY', 'STATE_CODE', 'ZIP_CODE']
        }
    },
    scd_type=1) }}
```

---

### dim_bridge()

Generates a Kimball bridge table for many-to-many relationship resolution.

```sql
{{ automate_dv_kimball.dim_bridge(
    src_pk,
    bridge_fks,
    source_model,
    src_ldts,
    weighting_factor,
    src_eff,
    src_exp,
    src_extra_columns) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Bridge primary key column |
| `bridge_fks` | list | Yes | -- | Foreign key columns to dimensions (e.g., `['CUSTOMER_HK', 'ACCOUNT_HK']`) |
| `source_model` | string | Yes | -- | Source bridge/link model |
| `src_ldts` | string | Yes | -- | Load datetime column |
| `weighting_factor` | string | No | none | Allocation/weighting column (e.g., `'OWNERSHIP_PCT'`) |
| `src_eff` | string | No | none | Effective-from column (for time-varying bridges) |
| `src_exp` | string | No | none | Expiry column (for time-varying bridges) |
| `src_extra_columns` | list | No | none | Additional columns to include |

**Example:**

```sql
{{ automate_dv_kimball.dim_bridge(
    src_pk='CUST_ACCT_HK',
    bridge_fks=['CUSTOMER_HK', 'ACCOUNT_HK'],
    source_model='bridge_cust_acct',
    src_ldts='LOAD_DATETIME',
    weighting_factor='OWNERSHIP_PCT') }}
```

---

## Fact Macros

### fact()

Generates a transaction fact table from a DV2 link and satellites.

```sql
{{ automate_dv_kimball.fact(
    src_pk,
    src_fk,
    src_ldts,
    source_model,
    dimensions,
    satellites,
    degenerate_dimensions,
    src_extra_columns) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Link hash key (fact grain) |
| `src_fk` | list | Yes | -- | FK columns on the link (must physically exist on the source model) |
| `src_ldts` | string | Yes | -- | Load datetime column |
| `source_model` | string | Yes | -- | Link model name |
| `dimensions` | dict | Yes | -- | Maps FK columns to dimension models (see below) |
| `satellites` | dict | No | none | Satellite configurations providing measures |
| `degenerate_dimensions` | list | No | none | Columns from source model included as-is (no join) |
| `src_extra_columns` | list | No | none | Additional columns |

**Dimension mapping:**

```yaml
dimensions:
  CUSTOMER_HK: dim_customer                              # simple FK -> dim
  PRODUCT_HK: dim_product                                # simple FK -> dim
  ORDER_DATE_KEY: {dim: dim_date, role: order_date}      # role-playing dimension
```

**Satellite config for facts:**

```yaml
satellites:
  sat_order_details:
    pk: ORDER_HK              # join key to the link
    ldts: LOAD_DATETIME        # used to get latest satellite row
    measures:                  # columns to include from this satellite
      - ORDER_AMOUNT
      - ORDER_QUANTITY
```

**Example:**

```sql
{{ automate_dv_kimball.fact(
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
        },
        'sat_order_context': {
            'pk': 'ORDER_HK',
            'ldts': 'LOAD_DATETIME',
            'measures': ['ORDER_DATE_KEY', 'ORDER_NUMBER']
        }
    }) }}
```

---

### fact_periodic_snapshot()

Generates a periodic snapshot fact table (e.g., daily inventory levels).

```sql
{{ automate_dv_kimball.fact_periodic_snapshot(
    src_pk,
    src_fk,
    src_ldts,
    source_model,
    dimensions,
    satellites,
    snapshot_period,
    src_extra_columns) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Entity key (from a hub, not a link) |
| `src_fk` | list | Yes | -- | FK columns. Set to `[]` if PK and FK are the same column |
| `src_ldts` | string | Yes | -- | Load datetime column |
| `source_model` | string | Yes | -- | Hub model name |
| `dimensions` | dict | Yes | -- | Dimension mappings |
| `satellites` | dict | Yes | -- | Satellite with snapshot measures |
| `snapshot_period` | string | Yes | -- | `'day'`, `'week'`, or `'month'` |
| `src_extra_columns` | list | No | none | Additional columns |

**Semi-additive measures:** Mark balance/level measures that should not be summed across time:

```yaml
satellites:
  sat_inventory_level:
    pk: PRODUCT_HK
    ldts: LOAD_DATETIME
    measures: [QUANTITY_ON_HAND]
    semi_additive: [QUANTITY_ON_HAND]    # latest snapshot only
```

**Example:**

```sql
{{ automate_dv_kimball.fact_periodic_snapshot(
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

---

### fact_accumulating_snapshot()

Generates an accumulating snapshot fact that tracks a lifecycle through milestone dates.

```sql
{{ automate_dv_kimball.fact_accumulating_snapshot(
    src_pk,
    src_fk,
    source_model,
    dimensions,
    milestones,
    lag_facts,
    satellites,
    timespan,
    src_extra_columns) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Link hash key (lifecycle grain) |
| `src_fk` | list | Yes | -- | FK columns on the link |
| `source_model` | string | Yes | -- | Link model name |
| `dimensions` | dict | Yes | -- | Dimension mappings |
| `milestones` | dict | Yes | -- | Milestone configurations (see below) |
| `lag_facts` | dict | Yes | -- | Computed DATEDIFF columns between milestones |
| `satellites` | dict | No | none | Additional satellite measures |
| `timespan` | bool | No | false | If true, adds TIMESPAN_DAYS (first milestone to last) |
| `src_extra_columns` | list | No | none | Additional columns |

**Milestone config:** Each milestone comes from a separate satellite.

```yaml
milestones:
  ORDER_DATE:                  # output column name
    model: sat_order_placed    # satellite providing this date
    pk: ORDER_HK              # join key
    date_col: LOAD_DATETIME    # the date column in the satellite
    dim: dim_date              # dimension for this date (optional)
  SHIP_DATE:
    model: sat_order_shipped
    pk: ORDER_HK
    date_col: LOAD_DATETIME
    dim: dim_date
```

**Lag facts:** Computed DATEDIFF between milestones. Can be NULL if a milestone hasn't been reached.

```yaml
lag_facts:
  ORDER_TO_SHIP_DAYS:
    from: ORDER_DATE
    to: SHIP_DATE
```

**Example (2 milestones):**

```sql
{{ automate_dv_kimball.fact_accumulating_snapshot(
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
    satellites={
        'sat_order_details': {
            'pk': 'ORDER_HK',
            'ldts': 'LOAD_DATETIME',
            'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
        }
    }) }}
```

---

### fact_factless()

Generates a factless fact table where the relationship intersection is the fact.

```sql
{{ automate_dv_kimball.fact_factless(
    src_pk,
    src_fk,
    src_ldts,
    source_model,
    dimensions,
    include_count,
    src_extra_columns) }}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `src_pk` | string | Yes | -- | Link hash key |
| `src_fk` | list | Yes | -- | FK columns to dimensions |
| `src_ldts` | string | Yes | -- | Load datetime column |
| `source_model` | string | Yes | -- | Link model name |
| `dimensions` | dict | Yes | -- | Dimension mappings |
| `include_count` | bool | No | true | Adds a `RECORD_COUNT` column |
| `src_extra_columns` | list | No | none | Additional columns |

**Example:**

```sql
{{ automate_dv_kimball.fact_factless(
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

---

## Bus Matrix Helpers

### bus_matrix()

Reads the `bus_matrix` variable from `dbt_project.yml` and returns a dict with `dimensions`, `facts`, and `bridges` keys.

```sql
{%- set bm = automate_dv_kimball.bus_matrix() -%}
{%- set dims = bm['dimensions'] -%}
{%- set facts = bm['facts'] -%}
```

### validate_star()

Run via `dbt run-operation automate_dv_kimball.validate_star`. Validates the bus matrix config with 5 structural error checks and 11 guidance warnings.

### generate_star()

Run via `dbt run-operation automate_dv_kimball.generate_star`. Prints the Enterprise Bus Matrix grid and model scaffolds for each dimension, fact, and bridge.

### get_dim_config(dim_name)

Returns the config dict for a specific dimension from the bus matrix.

### get_fact_config(fact_name)

Returns the config dict for a specific fact from the bus matrix.

### get_conformed_dimensions()

Returns only dimensions marked `conformed: true` in the bus matrix.

### get_dimensions_for_fact(fact_name)

Returns a list of dimension names referenced by a given fact.

---

## Generic Tests

### Data Vault 2.0 Tests

| Test | Applies to | Parameters | What it checks |
|------|-----------|------------|----------------|
| `hub_pk_unique_not_null` | Hub PK column | -- | PK is unique and not null |
| `hub_nk_not_null` | Hub NK column | -- | Natural key is not null |
| `link_fk_to_hub` | Link FK column | `hub_model`, `hub_pk` | FK resolves to a hub PK |
| `sat_hashdiff_not_null` | Satellite hashdiff | -- | Hashdiff is not null |
| `sat_pk_to_parent` | Satellite PK | `parent_model`, `parent_pk` | PK resolves to parent hub/link |
| `eff_sat_no_overlap` | Eff-sat PK | `start_date`, `end_date` | No overlapping date ranges |
| `eff_sat_no_gap` | Eff-sat PK | `start_date`, `end_date` | No gaps in date ranges |
| `ma_sat_cdk_unique` | MA-sat PK | `cdk_columns`, `ldts_column` | PK + CDK + LDTS is unique |
| `pit_pk_as_of_unique` | PIT PK | `as_of_column` | One row per entity per as-of date |
| `pit_covers_all_entities` | PIT PK | `hub_model`, `hub_pk` | PIT has entries for all hub entities |
| `xts_satellite_name_valid` | XTS satellite name | `valid_satellites` | Names match known satellite list |

### Kimball Tests

| Test | Applies to | Parameters | What it checks |
|------|-----------|------------|----------------|
| `dim_sk_unique_not_null` | Dimension PK | -- | Surrogate key is unique and not null |
| `dim_scd2_no_gap` | SCD2 dimension NK | `eff_from`, `eff_to` | No gaps in validity windows |
| `dim_scd2_one_current` | SCD2 dimension NK | `is_current_col` | Exactly one current row per entity |
| `fact_fk_to_dim` | Fact FK column | `dim_model`, `dim_pk` | FK resolves to dimension PK |
| `fact_measure_not_null` | Fact measure | -- | Measure is not null |
| `factless_no_duplicate` | Factless PK | -- | No duplicate grain rows |
| `periodic_snapshot_no_dup_grain` | Model-level | `grain_columns` | No duplicate rows at grain |
| `accum_snapshot_milestones_ordered` | Model-level | `milestone_columns` | Milestones are chronological |

**Usage example in schema.yml:**

```yaml
models:
  - name: dim_customer
    columns:
      - name: CUSTOMER_HK
        tests:
          - not_null
          - unique
          - automate_dv_kimball.dim_sk_unique_not_null

  - name: fact_order
    columns:
      - name: CUSTOMER_HK
        tests:
          - automate_dv_kimball.fact_fk_to_dim:
              dim_model: ref('dim_customer')
              dim_pk: CUSTOMER_HK
      - name: ORDER_AMOUNT
        tests:
          - automate_dv_kimball.fact_measure_not_null

  - name: fact_order_fulfillment
    tests:
      - automate_dv_kimball.accum_snapshot_milestones_ordered:
          milestone_columns: ['ORDER_DATE', 'SHIP_DATE', 'DELIVERY_DATE']
```
