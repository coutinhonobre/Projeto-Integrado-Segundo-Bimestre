with source as (

    select * from {{ source('wisdm_raw', 'wisdm_features') }}

)

select
    subject as subject_id,
    subject || '_' || activity_label || '_' || cast(window_id as varchar) as window_uid,
    window_id,
    window_start_sample,
    n_samples,
    activity_label,
    label_purity,

    accel_x_mean, accel_x_std, accel_x_min, accel_x_max,
    accel_y_mean, accel_y_std, accel_y_min, accel_y_max,
    accel_z_mean, accel_z_std, accel_z_min, accel_z_max,

    gyro_x_mean, gyro_x_std, gyro_x_min, gyro_x_max,
    gyro_y_mean, gyro_y_std, gyro_y_min, gyro_y_max,
    gyro_z_mean, gyro_z_std, gyro_z_min, gyro_z_max

from source
