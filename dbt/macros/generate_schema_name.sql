{#
  Override do macro padrao do dbt. Por padrao, quando um model define
  `+schema: X`, o dbt gera `<schema_do_profile>_X` (schema como sufixo). Aqui
  o comportamento e diferente de proposito: se o model define um schema
  customizado, ele vira o banco Glue INTEIRO (sem prefixo), permitindo um
  banco Glue dedicado por camada da medalhao (silver = schema do profile,
  gold = seu proprio banco). Padrao documentado no proprio dbt para esse caso
  ("Change the way dbt generates a schema name").
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
