/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro bigquery__fact(src_pk, src_fk, src_ldts, source_model, dimensions, satellites, degenerate_dimensions, src_extra_columns) -%}

    {{ automate_dv_kimball.default__fact(src_pk=src_pk, src_fk=src_fk,
                                 src_ldts=src_ldts,
                                 source_model=source_model,
                                 dimensions=dimensions,
                                 satellites=satellites,
                                 degenerate_dimensions=degenerate_dimensions,
                                 src_extra_columns=src_extra_columns) }}

{%- endmacro -%}
