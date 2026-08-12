-- Camada Gold: agregado de negocio no grao subject_id x activity_label.
-- Uma linha por combinacao sujeito/atividade, pronta para consumo direto no
-- dashboard (comparar sujeitos entre si e entre atividades) e como camada de
-- features agregadas para o modelo de ML (alem do grao fino de janela ja
-- disponivel em `fct_wisdm_windows`).

with windows as (

    select * from {{ ref('fct_wisdm_windows') }}

),

subject_totals as (

    -- total de janelas validas do sujeito (todas as atividades), usado para
    -- calcular a participacao percentual de cada atividade na sessao.
    select
        subject_id,
        count(*) as subject_n_windows
    from windows
    group by subject_id

),

aggregated as (

    select
        w.subject_id,
        w.activity_label,
        count(*) as n_windows,
        avg(w.label_purity) as avg_label_purity,

        avg(w.accel_x_mean) as accel_x_mean_avg,
        stddev(w.accel_x_mean) as accel_x_mean_stddev,
        avg(w.accel_y_mean) as accel_y_mean_avg,
        stddev(w.accel_y_mean) as accel_y_mean_stddev,
        avg(w.accel_z_mean) as accel_z_mean_avg,
        stddev(w.accel_z_mean) as accel_z_mean_stddev,

        avg(w.gyro_x_mean) as gyro_x_mean_avg,
        stddev(w.gyro_x_mean) as gyro_x_mean_stddev,
        avg(w.gyro_y_mean) as gyro_y_mean_avg,
        stddev(w.gyro_y_mean) as gyro_y_mean_stddev,
        avg(w.gyro_z_mean) as gyro_z_mean_avg,
        stddev(w.gyro_z_mean) as gyro_z_mean_stddev,

        -- Magnitude do VETOR DE MEDIAS por eixo (aproximacao simples de
        -- intensidade de movimento na janela). Nao e a media da magnitude
        -- instantanea (isso exigiria o sinal bruto, indisponivel aqui).
        avg(sqrt(power(w.accel_x_mean, 2) + power(w.accel_y_mean, 2) + power(w.accel_z_mean, 2)))
            as accel_mean_vector_magnitude_avg,
        avg(sqrt(power(w.gyro_x_mean, 2) + power(w.gyro_y_mean, 2) + power(w.gyro_z_mean, 2)))
            as gyro_mean_vector_magnitude_avg

    from windows w
    group by w.subject_id, w.activity_label

)

select
    a.subject_id || '_' || a.activity_label as subject_activity_uid,
    a.subject_id,
    a.activity_label,
    a.n_windows,
    round(cast(a.n_windows as double) / t.subject_n_windows, 4) as share_of_subject_windows,
    a.avg_label_purity,
    a.accel_x_mean_avg,
    a.accel_x_mean_stddev,
    a.accel_y_mean_avg,
    a.accel_y_mean_stddev,
    a.accel_z_mean_avg,
    a.accel_z_mean_stddev,
    a.gyro_x_mean_avg,
    a.gyro_x_mean_stddev,
    a.gyro_y_mean_avg,
    a.gyro_y_mean_stddev,
    a.gyro_z_mean_avg,
    a.gyro_z_mean_stddev,
    a.accel_mean_vector_magnitude_avg,
    a.gyro_mean_vector_magnitude_avg
from aggregated a
join subject_totals t on t.subject_id = a.subject_id
