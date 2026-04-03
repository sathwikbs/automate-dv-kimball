/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro bus_matrix() -%}

    {%- set config = var('bus_matrix', none) -%}

    {%- if config is none -%}
        {%- do return({'dimensions': {}, 'facts': {}, 'bridges': {}}) -%}
    {%- endif -%}

    {%- set dimensions = config.get('dimensions', {}) -%}
    {%- set facts = config.get('facts', {}) -%}
    {%- set bridges = config.get('bridges', {}) -%}

    {%- do return({
        'dimensions': dimensions,
        'facts': facts,
        'bridges': bridges
    }) -%}

{%- endmacro -%}


{%- macro get_dim_config(dim_name) -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set dim = bm['dimensions'].get(dim_name, none) -%}

    {%- if dim is none and execute -%}
        {%- do exceptions.warn("WARNING: Dimension '" ~ dim_name ~ "' not found in bus_matrix config.") -%}
    {%- endif -%}

    {%- do return(dim) -%}

{%- endmacro -%}


{%- macro get_fact_config(fact_name) -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set fact = bm['facts'].get(fact_name, none) -%}

    {%- if fact is none and execute -%}
        {%- do exceptions.warn("WARNING: Fact '" ~ fact_name ~ "' not found in bus_matrix config.") -%}
    {%- endif -%}

    {%- do return(fact) -%}

{%- endmacro -%}


{%- macro get_conformed_dimensions() -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set conformed = {} -%}

    {%- for dim_name, dim_config in bm['dimensions'].items() -%}
        {%- if dim_config.get('conformed', false) -%}
            {%- do conformed.update({dim_name: dim_config}) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- do return(conformed) -%}

{%- endmacro -%}


{%- macro get_dimensions_for_fact(fact_name) -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set fact = bm['facts'].get(fact_name, {}) -%}
    {%- set dim_refs = fact.get('dimensions', {}) -%}
    {%- set dim_names = [] -%}

    {%- for fk, dim_ref in dim_refs.items() -%}
        {%- if dim_ref is mapping -%}
            {%- set dim_name = dim_ref['dim'] -%}
        {%- else -%}
            {%- set dim_name = dim_ref -%}
        {%- endif -%}
        {%- if dim_name not in dim_names -%}
            {%- do dim_names.append(dim_name) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- do return(dim_names) -%}

{%- endmacro -%}
