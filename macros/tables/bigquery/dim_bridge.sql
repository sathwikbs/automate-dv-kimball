/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro bigquery__dim_bridge(src_pk, bridge_fks, source_model, src_ldts, weighting_factor, src_eff, src_exp, src_extra_columns) -%}

    {{ automate_dv_kimball.default__dim_bridge(src_pk=src_pk, bridge_fks=bridge_fks,
                                        source_model=source_model,
                                        src_ldts=src_ldts,
                                        weighting_factor=weighting_factor,
                                        src_eff=src_eff,
                                        src_exp=src_exp,
                                        src_extra_columns=src_extra_columns) }}

{%- endmacro -%}
