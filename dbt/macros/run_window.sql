{# 거래일 한정 (코인 프로젝트 run_anchor 와 같은 뜻). run_date 가 있으면 그 날만, 없으면 전부. #}
{% macro day_filter(col) -%}
    {%- set rd = var('run_date', none) -%}
    {%- if rd %}{{ col }} = date '{{ rd }}'{% else %}true{% endif -%}
{%- endmacro %}
