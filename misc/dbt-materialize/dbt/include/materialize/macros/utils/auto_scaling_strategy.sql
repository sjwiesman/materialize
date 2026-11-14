-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{#
Helpers for the `AUTO SCALING STRATEGY` cluster option. Its sub-policies are
independent and can be combined:

  - `on_hydration` (hydration burst) lets a managed cluster temporarily burst
    to a larger size while it has un-hydrated objects, then return to its
    steady size once hydration completes.
  - `on_load` lets Materialize move the cluster's SIZE within a range to keep
    up with its load.

The adapter models the strategy as a nested mapping:

  auto_scaling_strategy:
    on_hydration:
      hydration_size: '800cc'   # required; must differ from the cluster SIZE
      linger_duration: '15s'    # optional; default '0s'
    on_load:
      min_size: '50cc'          # required
      max_size: '800cc'         # required
      scale_down_after: '1h'    # optional

An empty mapping ({}) renders as `AUTO SCALING STRATEGY = ()`, which disables
autoscaling.
#}

{#
Validates the shape of an `auto_scaling_strategy` mapping and its
compatibility with the cluster's SIZE and schedule. Raises a compiler error
on invalid input. `size` and `schedule_type` are optional and only checked
when provided.
#}
{% macro internal_validate_auto_scaling_strategy(auto_scaling_strategy, size=none, schedule_type=none) %}
    {% if auto_scaling_strategy is not mapping %}
        {{ exceptions.raise_compiler_error(
            "auto_scaling_strategy must be a mapping, e.g. "
            ~ "{'on_hydration': {'hydration_size': '800cc', 'linger_duration': '15s'}}. "
            ~ "Got: " ~ auto_scaling_strategy
        ) }}
    {% endif %}

    {# AUTO SCALING STRATEGY is only supported on clusters with the default
       MANUAL schedule, matching the server-side restriction. #}
    {% if schedule_type is not none and schedule_type != 'manual' %}
        {{ exceptions.raise_compiler_error(
            "auto_scaling_strategy cannot be combined with a schedule_type "
            ~ "other than 'manual'. Got schedule_type: " ~ schedule_type
        ) }}
    {% endif %}

    {% for key in auto_scaling_strategy %}
        {% if key not in ['on_hydration', 'on_load'] %}
            {{ exceptions.raise_compiler_error(
                "Unknown auto_scaling_strategy key '" ~ key ~ "'. "
                ~ "The supported sub-policies are 'on_hydration' and 'on_load'."
            ) }}
        {% endif %}
    {% endfor %}

    {% if 'on_hydration' in auto_scaling_strategy %}
        {% set on_hydration = auto_scaling_strategy.get('on_hydration') %}
        {% if on_hydration is not mapping or not on_hydration.get('hydration_size') %}
            {{ exceptions.raise_compiler_error(
                "auto_scaling_strategy.on_hydration must be a mapping with a "
                ~ "'hydration_size' key, e.g. {'hydration_size': '800cc', "
                ~ "'linger_duration': '15s'}. Got: " ~ on_hydration
            ) }}
        {% endif %}
        {% for key in on_hydration %}
            {% if key not in ['hydration_size', 'linger_duration'] %}
                {{ exceptions.raise_compiler_error(
                    "Unknown auto_scaling_strategy.on_hydration key '" ~ key ~ "'. "
                    ~ "Supported keys: hydration_size, linger_duration."
                ) }}
            {% endif %}
        {% endfor %}
        {% if size is not none and on_hydration.get('hydration_size') == size %}
            {{ exceptions.raise_compiler_error(
                "auto_scaling_strategy.on_hydration.hydration_size must differ "
                ~ "from the cluster size '" ~ size ~ "'. A burst to the same "
                ~ "size is a no-op."
            ) }}
        {% endif %}
    {% endif %}

    {# The server checks that the range holds the cluster SIZE, which needs
       the ordering of sizes. #}
    {% if 'on_load' in auto_scaling_strategy %}
        {% set on_load = auto_scaling_strategy.get('on_load') %}
        {% if on_load is not mapping or not on_load.get('min_size') or not on_load.get('max_size') %}
            {{ exceptions.raise_compiler_error(
                "auto_scaling_strategy.on_load must be a mapping with 'min_size' "
                ~ "and 'max_size' keys, e.g. {'min_size': '50cc', "
                ~ "'max_size': '800cc', 'scale_down_after': '1h'}. Got: " ~ on_load
            ) }}
        {% endif %}
        {% for key in on_load %}
            {% if key not in ['min_size', 'max_size', 'scale_down_after'] %}
                {{ exceptions.raise_compiler_error(
                    "Unknown auto_scaling_strategy.on_load key '" ~ key ~ "'. "
                    ~ "Supported keys: min_size, max_size, scale_down_after."
                ) }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{#
Renders an `auto_scaling_strategy` mapping as the corresponding
`AUTO SCALING STRATEGY = (...)` clause. An empty mapping renders the
disabling form `AUTO SCALING STRATEGY = ()`.
#}
{% macro internal_render_auto_scaling_strategy(auto_scaling_strategy) %}
    {% if not auto_scaling_strategy %}
        {{ return("AUTO SCALING STRATEGY = ()") }}
    {% endif %}
    {% set policies = [] %}
    {% set on_hydration = auto_scaling_strategy.get('on_hydration') %}
    {% if on_hydration %}
        {% set parts = ["HYDRATION SIZE = " ~ dbt.string_literal(on_hydration.get('hydration_size'))] %}
        {% if on_hydration.get('linger_duration') is not none %}
            {% do parts.append("LINGER DURATION = " ~ dbt.string_literal(on_hydration.get('linger_duration'))) %}
        {% endif %}
        {% do policies.append("ON HYDRATION (" ~ parts | join(", ") ~ ")") %}
    {% endif %}
    {% set on_load = auto_scaling_strategy.get('on_load') %}
    {% if on_load %}
        {% set parts = [
            "MIN SIZE = " ~ dbt.string_literal(on_load.get('min_size')),
            "MAX SIZE = " ~ dbt.string_literal(on_load.get('max_size')),
        ] %}
        {% if on_load.get('scale_down_after') is not none %}
            {% do parts.append("SCALE DOWN AFTER = " ~ dbt.string_literal(on_load.get('scale_down_after'))) %}
        {% endif %}
        {% do policies.append("ON LOAD (" ~ parts | join(", ") ~ ")") %}
    {% endif %}
    {{ return("AUTO SCALING STRATEGY = (" ~ policies | join(", ") ~ ")") }}
{% endmacro %}

{#
Reads the configured autoscaling strategy of a cluster from
mz_internal.mz_cluster_auto_scaling_strategies and returns it as an
`auto_scaling_strategy` mapping, or none when the cluster has no strategy
configured (or the server predates the feature).

NOTE: the catalog stores durations as a {secs, nanos} object. The read-back
keeps whole seconds only, so sub-second components are dropped when the
strategy is copied to another cluster.

NOTE: mz_cluster_auto_scaling_strategies is a builtin materialized view
maintained on mz_catalog_server, so it reflects DDL with a small delay,
unlike mz_clusters. A strategy configured immediately before this read may
not be visible yet.
#}
{% macro internal_get_cluster_auto_scaling_strategy(cluster_name) %}
    {% set catalog_relation_exists %}
        SELECT count(*) > 0
        FROM mz_objects o
        JOIN mz_schemas s ON o.schema_id = s.id
        WHERE s.name = 'mz_internal'
            AND o.name = 'mz_cluster_auto_scaling_strategies'
    {% endset %}

    {% set exists_result = run_query(catalog_relation_exists) %}
    {% if not execute or not exists_result.rows[0][0] %}
        {{ return(none) }}
    {% endif %}

    {% set strategy_query %}
        SELECT
            s.strategy->'on_hydration'->>'hydration_size' AS hydration_size,
            (s.strategy->'on_hydration'->'linger_duration'->>'secs')::bigint AS linger_secs,
            s.strategy->'on_load'->>'min_size' AS min_size,
            s.strategy->'on_load'->>'max_size' AS max_size,
            (s.strategy->'on_load'->'scale_down_after'->>'secs')::bigint AS scale_down_secs
        FROM mz_internal.mz_cluster_auto_scaling_strategies s
        JOIN mz_clusters c ON c.id = s.cluster_id
        WHERE c.name = {{ dbt.string_literal(cluster_name) }}
    {% endset %}

    {% set strategy_result = run_query(strategy_query) %}
    {# A row can exist with a null strategy while a burst from a just-removed
       policy is still draining, so require a sub-policy's mandatory size. #}
    {% set strategy = {} %}
    {% if strategy_result.rows %}
        {% set row = strategy_result.rows[0] %}
        {% if row[0] is not none %}
            {% set on_hydration = {'hydration_size': row[0]} %}
            {% if row[1] is not none %}
                {% do on_hydration.update({'linger_duration': row[1] ~ 's'}) %}
            {% endif %}
            {% do strategy.update({'on_hydration': on_hydration}) %}
        {% endif %}
        {% if row[2] is not none %}
            {% set on_load = {'min_size': row[2], 'max_size': row[3]} %}
            {% if row[4] is not none %}
                {% do on_load.update({'scale_down_after': row[4] ~ 's'}) %}
            {% endif %}
            {% do strategy.update({'on_load': on_load}) %}
        {% endif %}
    {% endif %}
    {% if strategy %}
        {{ return(strategy) }}
    {% endif %}

    {{ return(none) }}
{% endmacro %}
