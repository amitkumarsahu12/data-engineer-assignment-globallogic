-- =========================================================
-- MODEL: dim_candidate
-- PURPOSE: Candidate dimension table
-- GRAIN: One record per candidate_id
-- IDEMPOTENT: Yes
-- =========================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_candidate (

    candidate_id TEXT PRIMARY KEY,

    first_name TEXT,

    last_name TEXT,

    email TEXT,

    phone TEXT,

    skills JSONB,

    education JSONB,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- =========================================================
-- LOAD / UPSERT
-- =========================================================

INSERT INTO warehouse.dim_candidate (

    candidate_id,
    first_name,
    last_name,
    email,
    phone,
    skills,
    education

)

SELECT

    c.candidate_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.skills,

    jsonb_agg(
        jsonb_build_object(
            'degree', e.degree,
            'institution', e.institution,
            'year', e.graduation_year
        )
    ) FILTER (
        WHERE e.degree IS NOT NULL
    ) AS education

FROM raw.raw_candidates c

LEFT OUTER JOIN raw.raw_education e
ON c.candidate_id = e.candidate_id

WHERE c.candidate_id IS NOT NULL

GROUP BY

    c.candidate_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.skills

ON CONFLICT (candidate_id)
DO UPDATE SET

    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    skills = EXCLUDED.skills,
    education = EXCLUDED.education,
    updated_at = CURRENT_TIMESTAMP;