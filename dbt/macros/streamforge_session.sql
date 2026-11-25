{#- Session safety for every dbt run.

    streamforge_assert_session (on-run-start), strictly in this order:
      1. USE SECONDARY ROLES NONE - the Snowflake user's default secondary roles are ALL,
         which would add ACCOUNTADMIN to the session;
      2. assert the primary role is STREAMFORGE_ROLE (never run as an admin role);
      3. assert secondary roles are really disabled.
    The checks run while the hook is rendered (run_query), which happens before dbt
    executes the hook's returned SQL - so the disabling statement is issued here first,
    not left to the returned SQL.

    Model pre-hooks (dbt_project.yml) then run "use secondary roles none" as the first
    statement on every model connection, followed by setting the session clock to UTC. -#}

{% macro streamforge_assert_session() %}
    {%- if execute -%}
        {%- do run_query("use secondary roles none") -%}
        {%- set result = run_query(
            "select current_role() as primary_role, current_secondary_roles() as secondary_roles"
        ) -%}
        {%- set role = result.columns[0].values()[0] -%}
        {%- set secondary = fromjson(result.columns[1].values()[0]) -%}
        {%- if (role | upper) != 'STREAMFORGE_ROLE' -%}
            {{ exceptions.raise_compiler_error(
                "StreamForge dbt must run as STREAMFORGE_ROLE, not " ~ role) }}
        {%- endif -%}
        {%- if secondary.get('roles', '') != '' -%}
            {{ exceptions.raise_compiler_error(
                "secondary roles are still active (" ~ secondary.get('roles') ~ "); refusing to run") }}
        {%- endif -%}
    {%- endif -%}
    use secondary roles none
{% endmacro %}
