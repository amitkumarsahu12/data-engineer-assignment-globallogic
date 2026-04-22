-- =========================================================
-- 1) How many jobs are currently open?
-- =========================================================

SELECT
    COUNT(*) AS open_jobs
FROM raw.raw_jobs
WHERE UPPER(status) = 'OPEN';



-- =========================================================
-- 2) Top 5 departments by number of applications
-- =========================================================

SELECT
    j.department,
    COUNT(a.application_id) AS total_applications
FROM raw.raw_jobs j
INNER JOIN raw.raw_applications a
    ON j.job_id = a.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;



-- =========================================================
-- 3) List candidates who applied to more than 3 jobs
-- =========================================================

SELECT
    candidate_id,
    COUNT(application_id) AS total_applications
FROM raw.raw_applications
GROUP BY candidate_id
HAVING COUNT(application_id) > 3
ORDER BY total_applications DESC;
