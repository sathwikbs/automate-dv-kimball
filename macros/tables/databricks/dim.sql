/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro databricks__dim(src_pk, src_nk, src_ldts, source_model, satellites, scd_type, src_extra_columns, integer_surrogate) -%}

    {{ automate_dv_kimball.default__dim(src_pk=src_pk, src_nk=src_nk,
                                src_ldts=src_ldts,
                                source_model=source_model,
                                satellites=satellites,
                                scd_type=scd_type,
                                src_extra_columns=src_extra_columns,
                                integer_surrogate=integer_surrogate) }}

{%- endmacro -%}
