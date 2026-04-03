/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro databricks__fact_accumulating_snapshot(src_pk, src_fk, source_model, dimensions, milestones, lag_facts, satellites, timespan, src_extra_columns) -%}

    {{ automate_dv_kimball.default__fact_accumulating_snapshot(src_pk=src_pk, src_fk=src_fk,
                                                       source_model=source_model,
                                                       dimensions=dimensions,
                                                       milestones=milestones,
                                                       lag_facts=lag_facts,
                                                       satellites=satellites,
                                                       timespan=timespan,
                                                       src_extra_columns=src_extra_columns) }}

{%- endmacro -%}
