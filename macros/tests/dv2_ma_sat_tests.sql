{# ── Multi-Active Satellite Generic Tests ── #}

{% test ma_sat_cdk_unique(model, column_name, cdk_columns, ldts_column='LOAD_DATETIME') %}
{# Validates that the combination of PK + CDK + LDTS is unique (no duplicate multi-active records).
   When applied at column level, column_name is the PK.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.ma_sat_cdk_unique:
               cdk_columns: ['PHONE_TYPE']
               ldts_column: LOAD_DATETIME
#}
    SELECT
        {{ column_name }}
        {%- for cdk in cdk_columns %},
        {{ cdk }}
        {%- endfor %},
        {{ ldts_column }},
        COUNT(*) AS row_count
    FROM {{ model }}
    GROUP BY
        {{ column_name }}
        {%- for cdk in cdk_columns %},
        {{ cdk }}
        {%- endfor %},
        {{ ldts_column }}
    HAVING COUNT(*) > 1
{% endtest %}
