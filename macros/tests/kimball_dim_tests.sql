{# ── Dimension Generic Tests ── #}

{% test dim_sk_unique_not_null(model, column_name) %}
{# Validates that a dimension surrogate key is unique and not null.
   Works for both hash-key and integer surrogate key dimensions.
   Usage:
     columns:
       - name: DIM_SK
         tests:
           - automate_dv_kimball.dim_sk_unique_not_null
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


{% test dim_scd2_no_gap(model, column_name, eff_from='EFFECTIVE_FROM', eff_to='EFFECTIVE_TO') %}
{# Validates that SCD2 dimension records have no gaps in their validity windows per entity.
   When applied at column level, column_name is the PK/NK to partition by.
   Uses automate_dv.max_datetime() for adapter-portable open-ended sentinel comparison.
   Usage:
     columns:
       - name: PRODUCT_HK
         tests:
           - automate_dv_kimball.dim_scd2_no_gap:
               eff_from: EFFECTIVE_FROM
               eff_to: EFFECTIVE_TO
#}
    WITH ordered AS (
        SELECT
            {{ column_name }},
            {{ eff_from }},
            {{ eff_to }},
            LEAD({{ eff_from }}) OVER (
                PARTITION BY {{ column_name }}
                ORDER BY {{ eff_from }}
            ) AS next_eff_from
        FROM {{ model }}
        WHERE {{ eff_to }} < {{ automate_dv.cast_datetime(automate_dv.max_datetime(), as_string=true) }}
    )

    SELECT {{ column_name }}
    FROM ordered
    WHERE next_eff_from IS NOT NULL
      AND {{ eff_to }} <> next_eff_from
{% endtest %}


{% test dim_scd2_one_current(model, column_name, is_current_col='IS_CURRENT') %}
{# Validates that each natural key has exactly one current record in an SCD2 dimension.
   When applied at column level, column_name is the PK/NK to partition by.
   Uses CAST to INT for SQL Server bit-type compatibility.
   Usage:
     columns:
       - name: PRODUCT_HK
         tests:
           - automate_dv_kimball.dim_scd2_one_current:
               is_current_col: IS_CURRENT
#}
    SELECT
        {{ column_name }},
        COUNT(*) AS current_count
    FROM {{ model }}
    WHERE CAST({{ is_current_col }} AS INT) = 1
    GROUP BY {{ column_name }}
    HAVING COUNT(*) <> 1
{% endtest %}
