{# ── Hub Generic Tests ── #}

{% test hub_pk_unique_not_null(model, column_name) %}
{# Validates that a hub primary key is both unique and not null.
   Returns any NULL PKs and any duplicate PK values.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.hub_pk_unique_not_null
#}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL

    UNION ALL

    SELECT {{ column_name }}
    FROM {{ model }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
{% endtest %}


{% test hub_nk_not_null(model, column_name) %}
{# Validates that a hub natural key is never null.
   Usage:
     columns:
       - name: CUSTOMER_ID
         tests:
           - automate_dv_kimball.hub_nk_not_null
#}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}
