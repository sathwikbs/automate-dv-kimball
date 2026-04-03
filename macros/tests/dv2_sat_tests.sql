{# ── Satellite Generic Tests ── #}

{% test sat_hashdiff_not_null(model, column_name) %}
{# Validates that a satellite hashdiff column is never null.
   Usage:
     columns:
       - name: HASHDIFF
         tests:
           - automate_dv_kimball.sat_hashdiff_not_null
#}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
{% endtest %}


{% test sat_pk_to_parent(model, column_name, parent_model, parent_pk) %}
{# Validates that every satellite PK resolves to its parent hub or link.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.sat_pk_to_parent:
               parent_model: ref('hub_customer')
               parent_pk: CUSTOMER_HK
#}
    SELECT s.{{ column_name }}
    FROM {{ model }} s
    LEFT JOIN {{ parent_model }} p
        ON s.{{ column_name }} = p.{{ parent_pk }}
    WHERE p.{{ parent_pk }} IS NULL
      AND s.{{ column_name }} IS NOT NULL
{% endtest %}
