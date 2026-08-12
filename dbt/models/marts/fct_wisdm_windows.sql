-- Grao: uma linha por janela de sinal (subject_id, activity_label, window_id).
-- Tabela final de consumo tanto para o treinamento/aplicacao do modelo de ML
-- (features + label) quanto para o dashboard (fatos por sujeito/atividade).
select * from {{ ref('stg_wisdm_features') }}
