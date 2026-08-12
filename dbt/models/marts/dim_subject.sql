-- 1 linha por sujeito, com contagem total de janelas validas e cobertura de
-- atividades. A contagem de janelas POR atividade (pedida na secao 4 do SDD)
-- fica no grao subject x activity de `gold_subject_activity_summary` - 18
-- colunas largas (uma por atividade) nao escalam bem aqui como escalavam
-- as 4 condicoes do WESAD anterior, e o formato tidy (uma linha por
-- combinacao) e mais direto tanto para o dashboard quanto para BI ad-hoc.
with windows as (

    select * from {{ ref('fct_wisdm_windows') }}

)

select
    subject_id,
    count(*) as n_windows,
    count(distinct activity_label) as n_distinct_activities,
    avg(label_purity) as avg_label_purity
from windows
group by subject_id
