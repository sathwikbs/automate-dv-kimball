{# ── Point-In-Time (PIT) Generic Tests ── #}

{% test pit_pk_as_of_unique(model, column_name, as_of_column='AS_OF_DATE') %}
{# Validates that each entity has at most one PIT record per as-of date.
   When applied at column level, column_name is the PK.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.pit_pk_as_of_unique:
               as_of_column: AS_OF_DATE
#}
    SELECT
        {{ column_name }},
        {{ as_of_column }},
        COUNT(*) AS row_count
    FROM {{ model }}
    GROUP BY {{ column_name }}, {{ as_of_column }}
    HAVING COUNT(*) > 1
{% endtest %}


{% test pit_covers_all_entities(model, column_name, hub_model, hub_pk) %}
{# Validates that the PIT table has at least one record for every entity in the hub.
   When applied at column level, column_name is the PK.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.pit_covers_all_entities:
               hub_model: ref('hub_customer')
               hub_pk: CUSTOMER_HK
#}
    SELECT h.{{ hub_pk }}
    FROM {{ hub_model }} h
    LEFT JOIN (
        SELECT DISTINCT {{ column_name }}
        FROM {{ model }}
    ) p ON h.{{ hub_pk }} = p.{{ column_name }}
    WHERE p.{{ column_name }} IS NULL
{% endtest %}
