{# ── Extended Tracking Satellite (XTS) Generic Tests ── #}

{% test xts_satellite_name_valid(model, column_name, valid_satellites) %}
{# Validates that satellite names in the XTS match a known list of satellites.
   Requires at least 1 entry in valid_satellites; returns no failures if empty.
   Usage:
     columns:
       - name: SATELLITE_NAME
         tests:
           - automate_dv_kimball.xts_satellite_name_valid:
               valid_satellites: ['SAT_CUSTOMER_DETAILS', 'SAT_CUSTOMER_ADDRESS']
#}
    {%- if valid_satellites | length < 1 %}
    SELECT 1 AS _dummy WHERE 1 = 0
    {%- else %}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND {{ column_name }} NOT IN (
        {%- for sat in valid_satellites %}
        '{{ sat }}'{{ "," if not loop.last }}
        {%- endfor %}
    )
    {%- endif %}
{% endtest %}
