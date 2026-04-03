{# ── Link Generic Tests ── #}

{% test link_fk_to_hub(model, column_name, hub_model, hub_pk) %}
{# Validates that every FK value in a link resolves to a hub PK.
   Referential integrity check for DV2 links.
   Usage:
     columns:
       - name: CUSTOMER_HK
         tests:
           - automate_dv_kimball.link_fk_to_hub:
               hub_model: ref('hub_customer')
               hub_pk: CUSTOMER_HK
#}
    SELECT l.{{ column_name }}
    FROM {{ model }} l
    LEFT JOIN {{ hub_model }} h
        ON l.{{ column_name }} = h.{{ hub_pk }}
    WHERE h.{{ hub_pk }} IS NULL
      AND l.{{ column_name }} IS NOT NULL
{% endtest %}
