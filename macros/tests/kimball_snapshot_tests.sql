{# ── Snapshot Fact Generic Tests ── #}

{% test accum_snapshot_milestones_ordered(model, milestone_columns) %}
{# Validates that accumulating snapshot milestone dates are in chronological order.
   Ignores NULL milestones (not yet reached).
   Requires at least 2 milestone columns; returns no failures if fewer are provided.
   Usage (model-level test in schema.yml):
     models:
       - name: fact_order_fulfillment
         tests:
           - automate_dv_kimball.accum_snapshot_milestones_ordered:
               milestone_columns: ['ORDER_DATE', 'SHIP_DATE', 'DELIVERY_DATE']
#}
    {%- if milestone_columns | length < 2 %}
    SELECT 1 AS _dummy WHERE 1 = 0
    {%- else %}
    SELECT *
    FROM {{ model }}
    WHERE
    {%- for i in range(milestone_columns | length - 1) %}
        {% if not loop.first %}AND {% endif -%}
        ({{ milestone_columns[i] }} IS NOT NULL
         AND {{ milestone_columns[i + 1] }} IS NOT NULL
         AND {{ milestone_columns[i] }} > {{ milestone_columns[i + 1] }})
    {%- endfor %}
    {%- endif %}
{% endtest %}


{% test periodic_snapshot_no_dup_grain(model, grain_columns) %}
{# Validates that a periodic snapshot has no duplicate rows at its grain.
   The grain is typically the combination of dimension keys + snapshot date.
   Requires at least 1 grain column; returns no failures if none are provided.
   Usage (model-level test in schema.yml):
     models:
       - name: fact_inventory_daily
         tests:
           - automate_dv_kimball.periodic_snapshot_no_dup_grain:
               grain_columns: ['PRODUCT_HK', 'snapshot_date_key']
#}
    {%- if grain_columns | length < 1 %}
    SELECT 1 AS _dummy WHERE 1 = 0
    {%- else %}
    SELECT
        {%- for col in grain_columns %}
        {{ col }}{{ "," if not loop.last }}
        {%- endfor %},
        COUNT(*) AS row_count
    FROM {{ model }}
    GROUP BY
        {%- for col in grain_columns %}
        {{ col }}{{ "," if not loop.last }}
        {%- endfor %}
    HAVING COUNT(*) > 1
    {%- endif %}
{% endtest %}
