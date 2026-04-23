SELECT
    j.department,
    f.job_id,

    ROUND(
        AVG(
            EXTRACT(
                DAY FROM
                f.hired_date - f.apply_date
            )
        ),
        2
    ) AS avg_time_to_hire_days

FROM warehouse.fct_applications f

INNER JOIN warehouse.dim_job j
ON f.job_id = j.job_id

WHERE f.is_hired = TRUE
AND COALESCE(f.is_hired_before_applied_anomaly, FALSE) = FALSE

GROUP BY
    j.department,
    f.job_id

ORDER BY
    avg_time_to_hire_days;
