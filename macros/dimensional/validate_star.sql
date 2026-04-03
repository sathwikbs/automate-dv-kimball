/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{#-
    validate_star()

    Run via: dbt run-operation validate_star

    Validates the bus_matrix config and provides opinionated guidance.
    Phase 1: 5 structural errors + 6 core guidance warnings
    Phase 2 (domain-specific): 6 additional checks (added incrementally)
-#}

{%- macro validate_star() -%}

    {%- set bm = automate_dv_kimball.bus_matrix() -%}
    {%- set dimensions = bm['dimensions'] -%}
    {%- set facts = bm['facts'] -%}
    {%- set bridges = bm['bridges'] -%}

    {%- set ns = namespace(errors=[], warnings=[]) -%}

    {%- if dimensions | length == 0 and facts | length == 0 -%}
        {{ log("No bus_matrix config found. Nothing to validate.", info=true) }}
        {%- do return(none) -%}
    {%- endif -%}

    {#- ===== PHASE 1: STRUCTURAL ERRORS ===== -#}

    {#- E1: FK Integrity -- every FK in a fact must reference a declared dimension -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set fact_dims = fact_config.get('dimensions', {}) -%}
        {%- for fk, dim_ref in fact_dims.items() -%}
            {%- if dim_ref is mapping -%}
                {%- set dim_name = dim_ref.get('dim', '') -%}
            {%- else -%}
                {%- set dim_name = dim_ref -%}
            {%- endif -%}
            {%- if dim_name not in dimensions -%}
                {%- do ns.errors.append("E1 FK_INTEGRITY: " ~ fact_name ~ "." ~ fk ~ " references dimension '" ~ dim_name ~ "' which is not declared in bus_matrix.dimensions") -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {#- E2: Duplicate Columns -- check for duplicate FK names within a single fact -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set fact_dims = fact_config.get('dimensions', {}) -%}
        {%- set seen_fks = [] -%}
        {%- for fk in fact_dims.keys() -%}
            {%- if fk in seen_fks -%}
                {%- do ns.errors.append("E2 DUPLICATE_COLUMN: " ~ fact_name ~ " has duplicate FK '" ~ fk ~ "'") -%}
            {%- else -%}
                {%- do seen_fks.append(fk) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {#- E3: Computed Expressions -- satellite measures should be column names, not expressions -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set sats = fact_config.get('satellites', {}) -%}
        {%- for sat_name, sat_config in sats.items() -%}
            {%- set measures = sat_config.get('measures', []) -%}
            {%- for m in measures -%}
                {%- if '(' in m or '+' in m or '-' in m or '*' in m or '/' in m -%}
                    {%- do ns.errors.append("E3 COMPUTED_EXPRESSION: " ~ fact_name ~ "." ~ sat_name ~ " measure '" ~ m ~ "' looks like an expression. Use a computed_sat or staging transform instead.") -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endfor -%}

    {#- E4: Grain Mismatch -- accumulating snapshots must have milestones -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- if fact_config.get('type', 'transaction') == 'accumulating_snapshot' -%}
            {%- if not fact_config.get('milestones') -%}
                {%- do ns.errors.append("E4 GRAIN_MISMATCH: " ~ fact_name ~ " is type 'accumulating_snapshot' but has no milestones defined") -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {#- E5: Supertype Key Mismatch -- subtype facts must reference their supertype -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- if fact_config.get('subtype_of') -%}
            {%- set parent = fact_config['subtype_of'] -%}
            {%- if parent not in facts -%}
                {%- do ns.errors.append("E5 SUPERTYPE_KEY_MISMATCH: " ~ fact_name ~ " declares subtype_of='" ~ parent ~ "' but that fact is not in bus_matrix.facts") -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {#- ===== PHASE 1: GUIDANCE WARNINGS ===== -#}

    {#- W1: PIT Recommendation -- SCD2 dims with 3+ satellites should consider PIT upstream -#}
    {%- for dim_name, dim_config in dimensions.items() -%}
        {%- set sats = dim_config.get('satellites', {}) -%}
        {%- if dim_config.get('scd_type', 1) == 2 and sats | length >= 3 -%}
            {%- do ns.warnings.append("W1 PIT_RECOMMENDATION: Dimension '" ~ dim_name ~ "' has " ~ sats | length ~ " satellites with SCD2. Consider using a PIT table upstream for query performance.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W2: Milestone Pivoting -- accumulating snapshots with 5+ milestones may need pivoting -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- if fact_config.get('type', 'transaction') == 'accumulating_snapshot' -%}
            {%- set milestones = fact_config.get('milestones', {}) -%}
            {%- if milestones | length >= 5 -%}
                {%- do ns.warnings.append("W2 MILESTONE_PIVOTING: " ~ fact_name ~ " has " ~ milestones | length ~ " milestones. Consider if some should be pivoted into a step dimension pattern.") -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W3: Same-As Link -- dims with same_as_link upstream should document dedup strategy -#}
    {%- for dim_name, dim_config in dimensions.items() -%}
        {%- set upstream = dim_config.get('recommended_upstream', '') -%}
        {%- if 'same_as_link' in upstream | lower -%}
            {%- do ns.warnings.append("W3 SAME_AS_LINK: Dimension '" ~ dim_name ~ "' uses same_as_link upstream. Ensure the dim model resolves to a single canonical record per natural key.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W4: Hashdiff -- SCD2 dims without hashdiff in satellite config -#}
    {%- for dim_name, dim_config in dimensions.items() -%}
        {%- if dim_config.get('scd_type', 1) == 2 -%}
            {%- set sats = dim_config.get('satellites', {}) -%}
            {%- for sat_name, sat_config in sats.items() -%}
                {%- if not sat_config.get('hashdiff') -%}
                    {%- do ns.warnings.append("W4 HASHDIFF: " ~ dim_name ~ "." ~ sat_name ~ " is SCD2 but has no 'hashdiff' column declared. Change detection may be unreliable.") -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W5: Conformed Dimension Consistency -- conformed dims used in multiple facts should have same FK name -#}
    {%- set conformed = automate_dv_kimball.get_conformed_dimensions() -%}
    {%- for dim_name in conformed -%}
        {%- set fk_names_per_fact = [] -%}
        {%- for fact_name, fact_config in facts.items() -%}
            {%- set fact_dims = fact_config.get('dimensions', {}) -%}
            {%- for fk, dim_ref in fact_dims.items() -%}
                {%- if dim_ref is mapping -%}
                    {%- set ref_dim = dim_ref.get('dim', '') -%}
                {%- else -%}
                    {%- set ref_dim = dim_ref -%}
                {%- endif -%}
                {%- if ref_dim == dim_name and dim_ref is not mapping -%}
                    {%- if fk not in fk_names_per_fact -%}
                        {%- do fk_names_per_fact.append(fk) -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
        {%- if fk_names_per_fact | length > 1 -%}
            {%- do ns.warnings.append("W5 CONFORMED_DIM_CONSISTENCY: Conformed dimension '" ~ dim_name ~ "' is referenced by different FK names across facts: " ~ fk_names_per_fact | join(', ') ~ ". Consider standardizing.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W6: Junk Dimension Suggestion -- facts with 4+ degenerate dimensions -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set degen = fact_config.get('degenerate_dimensions', []) -%}
        {%- if degen | length >= 4 -%}
            {%- do ns.warnings.append("W6 JUNK_DIM_SUGGESTION: " ~ fact_name ~ " has " ~ degen | length ~ " degenerate dimensions. Consider grouping low-cardinality flags/indicators into a junk dimension.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- ===== PHASE 2: DOMAIN-SPECIFIC CHECKS ===== -#}

    {#- W7: Semi-Additive Annotation -- periodic snapshots should declare semi_additive measures -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- if fact_config.get('type', 'transaction') == 'periodic_snapshot' -%}
            {%- set sats = fact_config.get('satellites', {}) -%}
            {%- set w7_ns = namespace(has_semi=false) -%}
            {%- for sat_name, sat_config in sats.items() -%}
                {%- if sat_config.get('semi_additive') -%}
                    {%- set w7_ns.has_semi = true -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if not w7_ns.has_semi -%}
                {%- do ns.warnings.append("W7 SEMI_ADDITIVE: " ~ fact_name ~ " is a periodic snapshot but no satellite declares semi_additive measures. Balance/level metrics should be annotated.") -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W8: Ghost Records -- accumulating snapshots should have ghost_record on milestone dims -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- if fact_config.get('type', 'transaction') == 'accumulating_snapshot' -%}
            {%- set milestones = fact_config.get('milestones', {}) -%}
            {%- for ms_name, ms_config in milestones.items() -%}
                {%- if ms_config is mapping -%}
                    {%- set ms_dim = ms_config.get('dim', '') -%}
                    {%- if ms_dim in dimensions -%}
                        {%- if not dimensions[ms_dim].get('ghost_record') -%}
                            {%- do ns.warnings.append("W8 GHOST_RECORD: " ~ fact_name ~ " milestone '" ~ ms_name ~ "' references dim '" ~ ms_dim ~ "' which has no ghost_record defined. NULL milestone FKs should point to a ghost/unknown member.") -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W9: Header/Line Grain -- facts with both header and line grain patterns -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set grain = fact_config.get('grain', '') -%}
        {%- if 'header' in grain | lower and 'line' in grain | lower -%}
            {%- do ns.warnings.append("W9 HEADER_LINE_GRAIN: " ~ fact_name ~ " grain appears to mix header and line levels. Consider separate header-level and line-level fact tables.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- W10: SCD2 Re-keying -- SCD2 dims used as FK in facts should use surrogate key -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set fact_dims = fact_config.get('dimensions', {}) -%}
        {%- for fk, dim_ref in fact_dims.items() -%}
            {%- if dim_ref is mapping -%}
                {%- set dim_name = dim_ref.get('dim', '') -%}
            {%- else -%}
                {%- set dim_name = dim_ref -%}
            {%- endif -%}
            {%- if dim_name in dimensions -%}
                {%- if dimensions[dim_name].get('scd_type', 1) == 2 -%}
                    {%- do ns.warnings.append("W10 SCD2_REKEYING: " ~ fact_name ~ "." ~ fk ~ " references SCD2 dimension '" ~ dim_name ~ "'. Ensure the FK points to the versioned surrogate key (DIM_SK or HK+EFFECTIVE_FROM), not just the natural key.") -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {#- W11: Currency Naming -- measures with 'amount' or 'price' should have currency context -#}
    {%- for fact_name, fact_config in facts.items() -%}
        {%- set sats = fact_config.get('satellites', {}) -%}
        {%- set w11_ns = namespace(has_money=false, has_currency_dim=false) -%}
        {%- for sat_name, sat_config in sats.items() -%}
            {%- set measures = sat_config.get('measures', []) -%}
            {%- for m in measures -%}
                {%- if 'amount' in m | lower or 'price' in m | lower or 'cost' in m | lower or 'revenue' in m | lower -%}
                    {%- set w11_ns.has_money = true -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
        {%- set fact_dims = fact_config.get('dimensions', {}) -%}
        {%- for fk, dim_ref in fact_dims.items() -%}
            {%- if dim_ref is mapping -%}
                {%- set dn = dim_ref.get('dim', '') -%}
            {%- else -%}
                {%- set dn = dim_ref -%}
            {%- endif -%}
            {%- if 'currency' in dn | lower -%}
                {%- set w11_ns.has_currency_dim = true -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if w11_ns.has_money and not w11_ns.has_currency_dim -%}
            {%- do ns.warnings.append("W11 CURRENCY_NAMING: " ~ fact_name ~ " has monetary measures but no currency dimension. Multi-currency environments need a currency dim or explicit currency column.") -%}
        {%- endif -%}
    {%- endfor -%}

    {#- ===== OUTPUT ===== -#}

    {{ log("", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("  VALIDATE STAR RESULTS", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("", info=true) }}

    {%- if ns.errors | length == 0 and ns.warnings | length == 0 -%}
        {{ log("All checks passed. No errors or warnings.", info=true) }}
    {%- endif -%}

    {%- if ns.errors | length > 0 -%}
        {{ log("ERRORS (" ~ ns.errors | length ~ "):", info=true) }}
        {{ log("-" * 40, info=true) }}
        {%- for err in ns.errors -%}
            {{ log("  [ERROR] " ~ err, info=true) }}
        {%- endfor -%}
        {{ log("", info=true) }}
    {%- endif -%}

    {%- if ns.warnings | length > 0 -%}
        {{ log("WARNINGS (" ~ ns.warnings | length ~ "):", info=true) }}
        {{ log("-" * 40, info=true) }}
        {%- for warn in ns.warnings -%}
            {{ log("  [WARN]  " ~ warn, info=true) }}
        {%- endfor -%}
        {{ log("", info=true) }}
    {%- endif -%}

    {{ log("Summary: " ~ ns.errors | length ~ " error(s), " ~ ns.warnings | length ~ " warning(s)", info=true) }}
    {{ log("=" * 80, info=true) }}

    {%- if ns.errors | length > 0 -%}
        {%- do exceptions.raise_compiler_error("validate_star found " ~ ns.errors | length ~ " error(s). Fix them before proceeding.") -%}
    {%- endif -%}

{%- endmacro -%}
