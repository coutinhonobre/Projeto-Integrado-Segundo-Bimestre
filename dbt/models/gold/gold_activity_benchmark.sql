-- Camada Gold: benchmark populacional por atividade (grao = activity_label),
-- usado como referencia no dashboard (ex. "como este sujeito se compara a
-- media da populacao andando?") e como baseline de normalizacao no
-- treinamento/avaliacao do modelo de ML.

with subject_activity as (

    select * from {{ ref('gold_subject_activity_summary') }}

)

select
    activity_label,
    count(distinct subject_id) as n_subjects,
    sum(n_windows) as n_windows,
    avg(accel_x_mean_avg) as accel_x_mean_avg,
    avg(accel_y_mean_avg) as accel_y_mean_avg,
    avg(accel_z_mean_avg) as accel_z_mean_avg,
    avg(gyro_x_mean_avg) as gyro_x_mean_avg,
    avg(gyro_y_mean_avg) as gyro_y_mean_avg,
    avg(gyro_z_mean_avg) as gyro_z_mean_avg,
    avg(accel_mean_vector_magnitude_avg) as accel_mean_vector_magnitude_avg,
    avg(gyro_mean_vector_magnitude_avg) as gyro_mean_vector_magnitude_avg
from subject_activity
group by activity_label
