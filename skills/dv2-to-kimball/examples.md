# Worked Examples: DV2 to Kimball

Each example shows the DV2 source assets, the Kimball target, and the complete model SQL.

---

## Example 1: Hub + Sat --> SCD1 Dimension

**Business question:** "Who are our customers and their current contact info?"

**DV2 sources:** `hub_customer` + `sat_customer_details`

**Pattern:** Hub provides identity (CUSTOMER_ID), satellite provides latest attributes (name, phone). SCD Type 1 = one row per customer, always current.

```sql
-- models/kimball/dim_customer.sql
{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_nk = 'CUSTOMER_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_customer' -%}

{%- set satellites = {
    'sat_customer_details': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE']
    }
} -%}

{{ automate_dv.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   satellites=satellites,
                   scd_type=1) }}
```

---

## Example 2: Hub + Sat + hashdiff --> SCD2 Dimension

**Business question:** "What was this product's name and price at any point in time?"

**DV2 sources:** `hub_product` + `sat_product_details`

**Pattern:** Same as SCD1 but with `hashdiff` and `scd_type=2`. Produces EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT columns for full history.

```sql
-- models/kimball/dim_product.sql
{{ config(materialized='table') }}

{%- set src_pk = 'PRODUCT_HK' -%}
{%- set src_nk = 'PRODUCT_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_product' -%}

{%- set satellites = {
    'sat_product_details': {
        'pk': 'PRODUCT_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['PRODUCT_NAME', 'UNIT_PRICE'],
        'hashdiff': 'HASHDIFF'
    }
} -%}

{{ automate_dv.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   satellites=satellites,
                   scd_type=2) }}
```

---

## Example 3: Link + Sats --> Transaction Fact

**Business question:** "How much did each customer order, for which products, on what date?"

**DV2 sources:** `link_order` + `sat_order_details` + `sat_order_context`

**Pattern:** Link provides the grain (one row per order). Satellites provide measures (amount, quantity) and context (date key, order number). Dimensions enforce star schema joins.

```sql
-- models/kimball/fact_order.sql
{{ config(materialized='table') }}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'PRODUCT_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'link_order' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer',
    'PRODUCT_HK': 'dim_product',
    'ORDER_DATE_KEY': {'dim': 'dim_date', 'role': 'order_date'}
} -%}

{%- set satellites = {
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
} -%}

{{ automate_dv.fact(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    source_model=source_model,
                    dimensions=dimensions,
                    satellites=satellites) }}
```

**Key points:**
- `src_fk` lists columns that exist on `link_order` itself (CUSTOMER_HK, PRODUCT_HK)
- `ORDER_DATE_KEY` comes from `sat_order_context` (not the link), so it's in `satellites.measures`
- Role-playing dimension: `{'dim': 'dim_date', 'role': 'order_date'}` means dim_date joined via ORDER_DATE_KEY

---

## Example 4: Link + Milestone Sats --> Accumulating Snapshot

**Business question:** "How long does it take from order placement to shipment?"

**DV2 sources:** `link_order` + `sat_order_placed` + `sat_order_shipped` + `sat_order_details`

**Pattern:** Each milestone comes from a separate satellite. Lag facts are computed DATEDIFF columns. Milestones can be NULL (not yet reached).

```sql
-- models/kimball/fact_order_fulfillment.sql
{{ config(materialized='table') }}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK'] -%}
{%- set source_model = 'link_order' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer'
} -%}

{%- set milestones = {
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
} -%}

{%- set lag_facts = {
    'ORDER_TO_SHIP_DAYS': {
        'from': 'ORDER_DATE',
        'to': 'SHIP_DATE'
    }
} -%}

{%- set satellites = {
    'sat_order_details': {
        'pk': 'ORDER_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
    }
} -%}

{{ automate_dv.fact_accumulating_snapshot(src_pk=src_pk,
                                          src_fk=src_fk,
                                          source_model=source_model,
                                          dimensions=dimensions,
                                          milestones=milestones,
                                          lag_facts=lag_facts,
                                          satellites=satellites) }}
```

**Key points:**
- One satellite per milestone: `sat_order_placed` for ORDER_DATE, `sat_order_shipped` for SHIP_DATE
- Lag facts are auto-computed as DATEDIFF between milestone pairs
- NULL milestones are expected (order placed but not yet shipped)
- Do NOT put `not_null` tests on lag fact columns

---

## Example 5: Hub + Periodic Sat --> Periodic Snapshot Fact

**Business question:** "What is the daily inventory level for each product?"

**DV2 sources:** `hub_product` + `sat_inventory_level`

**Pattern:** Hub provides grain (one row per product per snapshot). Satellite provides the snapshot measure. Semi-additive measures should NOT be summed across time.

```sql
-- models/kimball/fact_inventory_daily.sql
{{ config(materialized='table') }}

{%- set src_pk = 'PRODUCT_HK' -%}
{%- set src_fk = [] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_product' -%}

{%- set dimensions = {
    'PRODUCT_HK': 'dim_product'
} -%}

{%- set satellites = {
    'sat_inventory_level': {
        'pk': 'PRODUCT_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['QUANTITY_ON_HAND'],
        'semi_additive': ['QUANTITY_ON_HAND']
    }
} -%}

{{ automate_dv.fact_periodic_snapshot(src_pk=src_pk,
                                      src_fk=src_fk,
                                      src_ldts=src_ldts,
                                      source_model=source_model,
                                      dimensions=dimensions,
                                      satellites=satellites,
                                      snapshot_period='day') }}
```

**Key points:**
- `source_model` is a **hub** (not a link) because the grain is an entity, not a relationship
- `src_fk = []` because the PK and FK are the same column (PRODUCT_HK)
- `semi_additive` marks balance/level measures that should use latest-snapshot-only aggregation

---

## Example 6: Link (no measures) --> Factless Fact

**Business question:** "Which products were promoted to which customers?"

**DV2 sources:** `link_promotion_coverage`

**Pattern:** The link's existence IS the fact. No satellites needed. Optional RECORD_COUNT column for convenience.

```sql
-- models/kimball/fact_promotion_coverage.sql
{{ config(materialized='table') }}

{%- set src_pk = 'PROMO_COV_HK' -%}
{%- set src_fk = ['PRODUCT_HK', 'CUSTOMER_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'link_promotion_coverage' -%}

{%- set dimensions = {
    'PRODUCT_HK': 'dim_product',
    'CUSTOMER_HK': 'dim_customer'
} -%}

{{ automate_dv.fact_factless(src_pk=src_pk,
                              src_fk=src_fk,
                              src_ldts=src_ldts,
                              source_model=source_model,
                              dimensions=dimensions,
                              include_count=true) }}
```

---

## Example 7: NH-Link --> Transaction Fact (hand-built)

**Business question:** "What payments were made, by whom, for how much?"

**DV2 sources:** `nh_link_payment`

**Pattern:** Non-historized links are insert-only event records with payload denormalized on the link. Map directly to a transaction fact. Hand-built because NH-link payload is already on the source model.

```sql
-- models/kimball/fact_payment.sql
{{ config(materialized='table') }}

-- depends_on: {{ ref('dim_customer') }}

WITH nh AS (
    SELECT
        PAYMENT_HK,
        ORDER_HK,
        CUSTOMER_HK,
        PAYMENT_AMOUNT,
        PAYMENT_METHOD,
        LOAD_DATETIME
    FROM {{ ref('nh_link_payment') }}
)

SELECT
    nh.PAYMENT_HK,
    nh.ORDER_HK,
    nh.CUSTOMER_HK,
    nh.PAYMENT_AMOUNT,
    nh.PAYMENT_METHOD,
    nh.LOAD_DATETIME
FROM nh
WHERE nh.PAYMENT_HK IS NOT NULL
```

**Key points:**
- `-- depends_on` ensures dimensions load first even without a macro-generated join
- NH-link payload (PAYMENT_AMOUNT, PAYMENT_METHOD) becomes fact measures directly
- No satellite join needed: the NH-link IS the fact grain with measures

---

## Example 8: Hub + MA-Sat --> Pivoted Dimension (hand-built)

**Business question:** "What are each customer's current home, mobile, and work phone numbers?"

**DV2 sources:** `hub_customer` + `ma_sat_customer_phones`

**Pattern:** Multi-active satellite has multiple rows per customer (one per PHONE_TYPE). Pivot the latest value per type into one wide row per customer.

```sql
-- models/kimball/dim_customer_phone.sql
{{ config(materialized='table') }}

WITH latest_phones AS (
    SELECT
        CUSTOMER_HK,
        PHONE_TYPE,
        PHONE_NUMBER,
        LOAD_DATETIME
    FROM {{ ref('ma_sat_customer_phones') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_HK, PHONE_TYPE
        ORDER BY LOAD_DATETIME DESC
    ) = 1
),

pivoted AS (
    SELECT
        CUSTOMER_HK,
        MAX(CASE WHEN PHONE_TYPE = 'HOME' THEN PHONE_NUMBER END) AS HOME_PHONE,
        MAX(CASE WHEN PHONE_TYPE = 'MOBILE' THEN PHONE_NUMBER END) AS MOBILE_PHONE,
        MAX(CASE WHEN PHONE_TYPE = 'WORK' THEN PHONE_NUMBER END) AS WORK_PHONE,
        MAX(LOAD_DATETIME) AS LOAD_DATETIME
    FROM latest_phones
    GROUP BY CUSTOMER_HK
),

hub AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
)

SELECT
    h.CUSTOMER_HK,
    h.CUSTOMER_ID,
    p.HOME_PHONE,
    p.MOBILE_PHONE,
    p.WORK_PHONE,
    p.LOAD_DATETIME
FROM hub h
INNER JOIN pivoted p ON h.CUSTOMER_HK = p.CUSTOMER_HK
```

**Key points:**
- `QUALIFY ROW_NUMBER()` gets the latest phone per type (CDK = PHONE_TYPE)
- `MAX(CASE WHEN ...)` pivots multiple rows into columns
- Always join back to the hub for the business key (CUSTOMER_ID)
- The CDK values (HOME, MOBILE, WORK) must be known at design time for the pivot
