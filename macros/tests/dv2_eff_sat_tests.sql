{# ── Effectivity Satellite Generic Tests ── #}

{% test eff_sat_no_overlap(model, column_name, start_date='START_DATE', end_date='END_DATE') %}
{# Validates that no two effectivity records for the same entity have overlapping date ranges.
   When applied at column level, column_name is the PK to partition by.
   Usage:
     columns:
       - name: CUST_ACCT_HK
         tests:
           - automate_dv_kimball.eff_sat_no_overlap:
               start_date: START_DATE
               end_date: END_DATE
#}
    SELECT
        a.{{ column_name }}
    FROM {{ model }} a
    INNER JOIN {{ model }} b
        ON a.{{ column_name }} = b.{{ column_name }}
        AND a.{{ start_date }} <> b.{{ start_date }}
        AND a.{{ start_date }} < b.{{ end_date }}
        AND a.{{ end_date }} > b.{{ start_date }}
{% endtest %}


{% test eff_sat_no_gap(model, column_name, start_date='START_DATE', end_date='END_DATE') %}
{# Validates that effectivity date ranges have no gaps for a given entity.
   A gap exists when the end_date of one record does not equal the start_date of the next.
   Ignores open-ended records (end_date = max_datetime sentinel).
   When applied at column level, column_name is the PK to partition by.
   Uses automate_dv.max_datetime() for adapter-portable sentinel comparison.
   Usage:
     columns:
       - name: CUST_ACCT_HK
         tests:
           - automate_dv_kimball.eff_sat_no_gap:
               start_date: START_DATE
               end_date: END_DATE
#}
    WITH ordered AS (
        SELECT
            {{ column_name }},
            {{ start_date }},
            {{ end_date }},
            LEAD({{ start_date }}) OVER (
                PARTITION BY {{ column_name }}
                ORDER BY {{ start_date }}
            ) AS next_start
        FROM {{ model }}
        WHERE {{ end_date }} < {{ automate_dv.cast_datetime(automate_dv.max_datetime(), as_string=true) }}
    )

    SELECT {{ column_name }}
    FROM ordered
    WHERE next_start IS NOT NULL
      AND {{ end_date }} <> next_start
{% endtest %}
