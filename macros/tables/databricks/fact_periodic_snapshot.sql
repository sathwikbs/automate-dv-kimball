/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro databricks__fact_periodic_snapshot(src_pk, src_fk, src_ldts, source_model, dimensions, satellites, snapshot_period, src_extra_columns) -%}

    {{ automate_dv_kimball.default__fact_periodic_snapshot(src_pk=src_pk, src_fk=src_fk,
                                                    src_ldts=src_ldts,
                                                    source_model=source_model,
                                                    dimensions=dimensions,
                                                    satellites=satellites,
                                                    snapshot_period=snapshot_period,
                                                    src_extra_columns=src_extra_columns) }}

{%- endmacro -%}
