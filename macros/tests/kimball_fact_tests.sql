{# ── Fact Table Generic Tests ── #}

{% test fact_fk_to_dim(model, column_name, dim_model, dim_pk) %}
{# Validates that every FK in a fact table resolves to a dimension PK/SK.
   Referential integrity check for Kimball star schemas.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.fact_fk_to_dim:
               dim_model: ref('dim_customer')
               dim_pk: CUSTOMER_HK
#}
    SELECT f.{{ column_name }}
    FROM {{ model }} f
    LEFT JOIN {{ dim_model }} d
        ON f.{{ column_name }} = d.{{ dim_pk }}
    WHERE d.{{ dim_pk }} IS NULL
      AND f.{{ column_name }} IS NOT NULL
{% endtest %}


{% test fact_measure_not_null(model, column_name) %}
{# Validates that a fact measure column is never null.
   Null measures in transaction facts typically indicate data quality issues.
   Usage:
     columns:
       - name: ORDER_AMOUNT
         tests:
           - automate_dv_kimball.fact_measure_not_null
#}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}


{% test factless_no_duplicate(model, column_name) %}
{# Validates that a factless fact has no duplicate grain rows on a single PK column.
   When applied at column level, column_name is the PK to check for duplicates.
   For composite grain uniqueness, use periodic_snapshot_no_dup_grain instead.
   Each intersection should appear exactly once.
   Usage:
     columns:
       - name: PROMO_COV_HK
         tests:
           - automate_dv_kimball.factless_no_duplicate
#}
    SELECT {{ column_name }}
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
{% endtest %}
