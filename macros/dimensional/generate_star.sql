/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{#-
    generate_star()

    Run via: dbt run-operation generate_star

    Reads the bus_matrix config from dbt_project.yml vars and:
    1. Prints the Enterprise Bus Matrix grid (facts x dimensions)
    2. Prints recommended model SQL scaffolds for each dim and fact
-#}

{%- macro generate_star() -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set dimensions = bm['dimensions'] -%}
    {%- set facts = bm['facts'] -%}
    {%- set bridges = bm['bridges'] -%}

    {%- if dimensions | length == 0 and facts | length == 0 -%}
        {{ log("No bus_matrix config found in dbt_project.yml vars. Nothing to generate.", info=true) }}
        {%- do return(none) -%}
    {%- endif -%}

    {#- ===== ENTERPRISE BUS MATRIX GRID ===== -#}

    {{ log("", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("  ENTERPRISE BUS MATRIX", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("", info=true) }}

    {%- set dim_names = dimensions.keys() | list -%}
    {%- set max_fact_len = 30 -%}

    {#- Header row -#}
    {%- set header = "%-30s" | format("") -%}
    {%- for dim_name in dim_names -%}
        {%- set header = header ~ " | %-14s" | format(dim_name[:14]) -%}
    {%- endfor -%}
    {{ log(header ~ " |", info=true) }}

    {%- set separator = "-" * 30 -%}
    {%- for dim_name in dim_names -%}
        {%- set separator = separator ~ "-+-" ~ "-" * 14 -%}
    {%- endfor -%}
    {{ log(separator ~ "-+", info=true) }}

    {#- Fact rows -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set row = "%-30s" | format(fact_name[:30]) -%}
        {%- set fact_dims = fact_config.get('dimensions', {}) -%}

        {%- for dim_name in dim_names -%}
            {%- set role_count = namespace(count=0) -%}
            {%- for fk, dim_ref in fact_dims.items() -%}
                {%- if dim_ref is mapping -%}
                    {%- if dim_ref.get('dim', '') == dim_name -%}
                        {%- set role_count.count = role_count.count + 1 -%}
                    {%- endif -%}
                {%- elif dim_ref == dim_name -%}
                    {%- set role_count.count = role_count.count + 1 -%}
                {%- endif -%}
            {%- endfor -%}

            {%- if role_count.count > 1 -%}
                {%- set cell = "X(%d)" | format(role_count.count) -%}
            {%- elif role_count.count == 1 -%}
                {%- set cell = "X" -%}
            {%- else -%}
                {%- set cell = "" -%}
            {%- endif -%}
            {%- set row = row ~ " | %-14s" | format(cell) -%}
        {%- endfor -%}

        {{ log(row ~ " |", info=true) }}
    {%- endfor -%}

    {{ log("", info=true) }}
    {{ log("X(N) = role-playing dimension (appears N times on that fact)", info=true) }}
    {{ log("", info=true) }}

    {#- ===== DIMENSION MODEL SCAFFOLDS ===== -#}

    {{ log("=" * 80, info=true) }}
    {{ log("  DIMENSION MODEL SCAFFOLDS", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("", info=true) }}

    {%- for dim_name, dim_config in dimensions.items() -%}
        {%- set dim_type = dim_config.get('type', 'standard') -%}

        {%- if dim_type == 'static' -%}
            {{ log("-- " ~ dim_name ~ " (static -- use seed or ref_table directly)", info=true) }}
            {{ log("-- source_model: " ~ dim_config.get('source_model', 'unknown'), info=true) }}
        {%- else -%}
            {{ log("-- " ~ dim_name, info=true) }}
            {%- if dim_config.get('recommended_upstream') %}
            {{ log("-- Recommended upstream: " ~ dim_config['recommended_upstream'], info=true) }}
            {%- endif %}
            {{ log("-- {{ config(materialized='incremental', unique_key='" ~ dim_config.get('natural_key', 'PK') ~ "') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.dim(", info=true) }}
            {{ log("--     src_pk='" ~ (dim_config.get('satellites', {}).values() | first | default({})).get('pk', 'HK') ~ "',", info=true) }}
            {{ log("--     src_nk='" ~ dim_config.get('natural_key', 'NK') ~ "',", info=true) }}
            {{ log("--     src_ldts='LOAD_DATETIME',", info=true) }}
            {{ log("--     source_model='" ~ dim_config.get('source_model', 'hub_xxx') ~ "',", info=true) }}
            {{ log("--     satellites={...},", info=true) }}
            {{ log("--     scd_type=" ~ dim_config.get('scd_type', 1) ~ ") }}", info=true) }}
            {%- if dim_config.get('conformed', false) %}
            {{ log("-- NOTE: Conformed dimension -- shared across multiple facts", info=true) }}
            {%- endif %}
        {%- endif -%}
        {{ log("", info=true) }}
    {%- endfor -%}

    {#- ===== FACT MODEL SCAFFOLDS ===== -#}

    {{ log("=" * 80, info=true) }}
    {{ log("  FACT MODEL SCAFFOLDS", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("", info=true) }}

    {%- for fact_name, fact_config in facts.items() -%}
        {%- set fact_type = fact_config.get('type', 'transaction') -%}

        {{ log("-- " ~ fact_name ~ " (" ~ fact_type ~ ")", info=true) }}
        {%- if fact_config.get('recommended_upstream') %}
        {{ log("-- Recommended upstream: " ~ fact_config['recommended_upstream'], info=true) }}
        {%- endif %}

        {%- if fact_type == 'transaction' -%}
            {{ log("-- {{ config(materialized='incremental', unique_key='" ~ fact_config.get('grain', 'PK') ~ "') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.fact(...) }}", info=true) }}
        {%- elif fact_type == 'periodic_snapshot' -%}
            {{ log("-- {{ config(materialized='incremental') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.fact_periodic_snapshot(..., snapshot_period='" ~ fact_config.get('snapshot_period', 'day') ~ "') }}", info=true) }}
            {%- if fact_config.get('supertype', false) %}
            {{ log("-- NOTE: Supertype fact -- subtype facts should use subtype_of annotation", info=true) }}
            {%- endif %}
            {%- if fact_config.get('subtype_of') %}
            {{ log("-- NOTE: Subtype of " ~ fact_config['subtype_of'] ~ " -- must use same surrogate keys", info=true) }}
            {%- endif %}
        {%- elif fact_type == 'accumulating_snapshot' -%}
            {{ log("-- {{ config(materialized='incremental') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.fact_accumulating_snapshot(...) }}", info=true) }}
        {%- elif fact_type == 'factless' -%}
            {{ log("-- {{ config(materialized='incremental') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.fact_factless(...) }}", info=true) }}
        {%- elif fact_type == 'consolidated' -%}
            {{ log("-- {{ config(materialized='incremental') }}", info=true) }}
            {{ log("-- Consolidated from: " ~ fact_config.get('source_facts', []) | join(', '), info=true) }}
            {{ log("-- {{ automate_dv_kimball.fact(...) }}  -- use standard fact() with pre-joined upstream", info=true) }}
        {%- endif -%}

        {{ log("", info=true) }}
    {%- endfor -%}

    {#- ===== BRIDGE SCAFFOLDS ===== -#}

    {%- if bridges | length > 0 -%}
        {{ log("=" * 80, info=true) }}
        {{ log("  BRIDGE TABLE SCAFFOLDS", info=true) }}
        {{ log("=" * 80, info=true) }}
        {{ log("", info=true) }}

        {%- for bridge_name, bridge_config in bridges.items() -%}
            {{ log("-- " ~ bridge_name, info=true) }}
            {{ log("-- {{ config(materialized='incremental') }}", info=true) }}
            {{ log("-- {{ automate_dv_kimball.dim_bridge(", info=true) }}
            {{ log("--     src_pk='...',", info=true) }}
            {{ log("--     bridge_fks=" ~ bridge_config.get('dimensions', []) ~ ",", info=true) }}
            {{ log("--     source_model='" ~ bridge_config.get('source_model', 'bridge_xxx') ~ "',", info=true) }}
            {%- if bridge_config.get('weighting_factor') %}
            {{ log("--     weighting_factor='" ~ bridge_config['weighting_factor'] ~ "',", info=true) }}
            {%- endif %}
            {{ log("--     src_ldts='LOAD_DATETIME') }}", info=true) }}
            {{ log("", info=true) }}
        {%- endfor -%}
    {%- endif -%}

    {{ log("=" * 80, info=true) }}
    {{ log("  Generation complete. " ~ dimensions | length ~ " dimensions, " ~ facts | length ~ " facts, " ~ bridges | length ~ " bridges.", info=true) }}
    {{ log("=" * 80, info=true) }}

{%- endmacro -%}
