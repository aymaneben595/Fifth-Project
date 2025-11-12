-- =============================================================
-- HR DATA PIPELINE (UTF-8 CLEAN VERSION)
-- PostgreSQL 17 compatible
-- =============================================================
/*
CREATE DATABASE fdb;
-- -------------------------------------------------------------
-- 0. Schema
-- -------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS hr;

-- -------------------------------------------------------------
-- 1. Raw table (structure must match CSV columns)
-- -------------------------------------------------------------
DROP TABLE IF EXISTS hr.raw_employees CASCADE;

CREATE TABLE hr.raw_employees (
    employee_name TEXT,
    empid TEXT PRIMARY KEY,
    marriedid INT,
    maritalstatusid INT,
    genderid INT,
    empstatusid INT,
    deptid INT,
    perfscoreid INT,
    fromdiversityjobfairid INT,
    salary TEXT,
    termd INT,
    positionid INT,
    position TEXT,
    state TEXT,
    zip TEXT,
    dob TEXT,
    sex TEXT,
    maritaldesc TEXT,
    citizendesc TEXT,
    hispaniclatino TEXT,
    racedesc TEXT,
    dateofhire TEXT,
    dateoftermination TEXT,
    termreason TEXT,
    employmentstatus TEXT,
    department TEXT,
    managername TEXT,
    managerid TEXT,
    recruitmentsource TEXT,
    performancescore TEXT,
    engagementsurvey TEXT,
    empsatisfaction TEXT,
    specialprojectscount INT,
    lastperformancereview_date TEXT,
    dayslatelast30 INT,
    absences INT
);

-- Uncomment and adjust path before running
-- COPY hr.raw_employees
-- FROM 'C:\Users\ayman\OneDrive\Desktop\New folder\VSCode, SQL & Python\CSV\HRDataset_v14(in).csv'
-- DELIMITER ',' CSV HEADER ENCODING 'UTF8';
*/

-- -------------------------------------------------------------
-- 2. Utility functions
-- -------------------------------------------------------------
DROP FUNCTION IF EXISTS hr.safe_date(TEXT);
CREATE OR REPLACE FUNCTION hr.safe_date(input TEXT)
RETURNS DATE AS $$
DECLARE
    t DATE;
BEGIN
    IF input IS NULL OR trim(input) = '' THEN
        RETURN NULL;
    END IF;

    BEGIN
        t := to_date(trim(input), 'MM/DD/YYYY');
        RETURN t;
    EXCEPTION WHEN others THEN
        NULL;
    END;

    BEGIN
        t := to_date(trim(input), 'YYYY-MM-DD');
        RETURN t;
    EXCEPTION WHEN others THEN
        NULL;
    END;

    BEGIN
        t := to_date(trim(input), 'DD/MM/YYYY');
        RETURN t;
    EXCEPTION WHEN others THEN
        NULL;
    END;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS hr.safe_numeric(TEXT);
CREATE OR REPLACE FUNCTION hr.safe_numeric(input TEXT)
RETURNS NUMERIC AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF input IS NULL OR trim(input) = '' THEN
        RETURN NULL;
    END IF;
    cleaned := regexp_replace(input, '[^0-9.\-]', '', 'g');
    IF cleaned = '' THEN
        RETURN NULL;
    END IF;
    RETURN cleaned::NUMERIC;
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- -------------------------------------------------------------
-- 3. fact_employee_clean
-- -------------------------------------------------------------
DROP TABLE IF EXISTS hr.fact_employee_clean CASCADE;

CREATE TABLE hr.fact_employee_clean AS
SELECT
    row_number() OVER () AS surrogate_id,
    trim(employee_name) AS employee_name,
    trim(empid) AS employee_id,

    -- ✅ NEW: Include Race
    trim(racedesc) AS racedesc,

    CASE
        WHEN lower(coalesce(nullif(trim(sex),''),'x')) LIKE 'f%' THEN 'Female'
        WHEN lower(coalesce(nullif(trim(sex),''),'x')) LIKE 'm%' THEN 'Male'
        WHEN genderid IN (0) THEN 'Female'
        WHEN genderid IN (1) THEN 'Male'
        ELSE 'Unknown'
    END AS gender,
    hr.safe_numeric(salary) AS salary,
    trim(position) AS position,
    COALESCE(NULLIF(trim(department),''), 'Unknown') AS department,
    COALESCE(NULLIF(trim(managername),''), 'No Manager') AS manager_name,
    hr.safe_date(dob) AS date_of_birth,
    hr.safe_date(dateofhire) AS date_of_hire,
    hr.safe_date(dateoftermination) AS date_of_termination,
    COALESCE(NULLIF(trim(termreason),''), 'Not Terminated') AS termination_reason,
    COALESCE(NULLIF(trim(recruitmentsource),''), 'Unknown') AS recruitment_source,
    GREATEST(LEAST(hr.safe_numeric(engagementsurvey), 5), 0) AS engagement_survey,
    GREATEST(LEAST(hr.safe_numeric(empsatisfaction), 5), 0) AS emp_satisfaction,
    COALESCE(specialprojectscount, 0) AS special_projects_count,
    COALESCE(dayslatelast30, 0) AS days_late_last_30,
    COALESCE(absences, 0) AS absences,
    CASE
        WHEN lower(coalesce(performancescore,'')) ~ 'exceed|excel' THEN 'High'
        WHEN lower(coalesce(performancescore,'')) ~ 'fully|meet|satisf' THEN 'Medium'
        WHEN lower(coalesce(performancescore,'')) ~ 'need|improv|poor' THEN 'Low'
        ELSE 'Unknown'
    END AS performance_category,
    CASE
        WHEN coalesce(termd, 0) = 1 THEN TRUE
        WHEN hr.safe_date(dateoftermination) IS NOT NULL THEN TRUE
        WHEN lower(coalesce(employmentstatus,'')) LIKE '%termin%' THEN TRUE
        ELSE FALSE
    END AS attrition_flag,

    CASE
        WHEN hr.safe_date(dateofhire) IS NULL THEN NULL
        ELSE (COALESCE(hr.safe_date(dateoftermination), CURRENT_DATE) - hr.safe_date(dateofhire))
    END AS tenure_days,

    CASE
        WHEN hr.safe_date(dateofhire) IS NULL THEN NULL
        ELSE ROUND(((COALESCE(hr.safe_date(dateoftermination), CURRENT_DATE) - hr.safe_date(dateofhire)) / 365.25)::numeric, 2)
    END AS tenure_years,

    CASE
        WHEN hr.safe_date(dob) IS NOT NULL AND hr.safe_date(dateofhire) IS NOT NULL
        THEN FLOOR((hr.safe_date(dateofhire) - hr.safe_date(dob)) / 365.25)
        ELSE NULL
    END AS age_at_hire,

    performancescore AS performancescore_raw,
    dateofhire AS dateofhire_raw,
    dateoftermination AS dateoftermination_raw
FROM hr.raw_employees
ORDER BY empid NULLS LAST;

ALTER TABLE hr.fact_employee_clean ADD COLUMN id BIGSERIAL PRIMARY KEY;

CREATE INDEX IF NOT EXISTS idx_hr_fact_empid ON hr.fact_employee_clean(employee_id);
CREATE INDEX IF NOT EXISTS idx_hr_fact_dept ON hr.fact_employee_clean(department);
CREATE INDEX IF NOT EXISTS idx_hr_fact_mgr ON hr.fact_employee_clean(manager_name);
CREATE INDEX IF NOT EXISTS idx_hr_fact_attrition ON hr.fact_employee_clean(attrition_flag);
CREATE INDEX IF NOT EXISTS idx_hr_fact_race ON hr.fact_employee_clean(racedesc);

-- -------------------------------------------------------------
-- 4. Dimension tables
-- -------------------------------------------------------------
DROP TABLE IF EXISTS hr.dim_department CASCADE;
CREATE TABLE hr.dim_department AS
SELECT md5(coalesce(department,'')) AS department_key, department
FROM (SELECT DISTINCT department FROM hr.fact_employee_clean) t;
ALTER TABLE hr.dim_department ADD PRIMARY KEY (department_key);

DROP TABLE IF EXISTS hr.dim_manager CASCADE;
CREATE TABLE hr.dim_manager AS
SELECT md5(coalesce(manager_name,'')) AS manager_key, manager_name
FROM (SELECT DISTINCT manager_name FROM hr.fact_employee_clean) t;
ALTER TABLE hr.dim_manager ADD PRIMARY KEY (manager_key);

DROP TABLE IF EXISTS hr.dim_position CASCADE;
CREATE TABLE hr.dim_position AS
SELECT md5(coalesce(position,'')) AS position_key, position
FROM (SELECT DISTINCT position FROM hr.fact_employee_clean) t;
ALTER TABLE hr.dim_position ADD PRIMARY KEY (position_key);

-- ✅ NEW (Optional): Race dimension
DROP TABLE IF EXISTS hr.dim_race CASCADE;
CREATE TABLE hr.dim_race AS
SELECT md5(coalesce(racedesc,'')) AS race_key, racedesc AS race_name
FROM (SELECT DISTINCT racedesc FROM hr.fact_employee_clean) t;
ALTER TABLE hr.dim_race ADD PRIMARY KEY (race_key);

-- -------------------------------------------------------------
-- 5. Next steps (optional sections)
-- -------------------------------------------------------------
--  - dim_date
--  - snapshot_employee_monthly
--  - metrics tables
--  - materialized views
--  - QA or exports
-- Core pipeline is complete and Power BI ready.
