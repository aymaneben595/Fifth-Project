-- =============================================================
-- 🧭 HR DATA PIPELINE — EXPLAINED VERSION (PostgreSQL 17)
-- =============================================================
-- This script cleans and organizes the HR dataset for analysis and dashboards.
-- It converts messy CSV data into structured tables ready for Power BI or reports.
-- =============================================================


/*
-- -------------------------------------------------------------
-- 🏗️ STEP 0: Create database and schema
-- -------------------------------------------------------------
-- A database called "fdb" will store everything.
-- The "hr" schema is a folder inside it where all HR tables will live.
CREATE DATABASE fdb;

CREATE SCHEMA IF NOT EXISTS hr;

-- -------------------------------------------------------------
-- 🧾 STEP 1: Import the raw HR data from CSV
-- -------------------------------------------------------------
-- This table exactly matches the columns in the CSV file.
-- It holds the uncleaned, raw HR data before transformation.
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

-- 🧩 When ready, use the COPY command below to load your CSV file.
-- Replace the path with your own file location.
-- COPY hr.raw_employees
-- FROM 'C:\path\to\HRDataset.csv'
-- DELIMITER ',' CSV HEADER ENCODING 'UTF8';
*/


-- -------------------------------------------------------------
-- 🧹 STEP 2: Helper functions for cleaning data
-- -------------------------------------------------------------

-- 💬 safe_date() tries to fix and convert text dates into real date format (YYYY-MM-DD).
-- If a value doesn’t look like a date, it returns NULL instead of failing.
DROP FUNCTION IF EXISTS hr.safe_date(TEXT);
CREATE OR REPLACE FUNCTION hr.safe_date(input TEXT)
RETURNS DATE AS $$
DECLARE
    t DATE;
BEGIN
    IF input IS NULL OR trim(input) = '' THEN
        RETURN NULL;
    END IF;

    -- Try multiple date formats commonly found in HR files
    BEGIN
        t := to_date(trim(input), 'MM/DD/YYYY');
        RETURN t;
    EXCEPTION WHEN others THEN NULL;
    END;

    BEGIN
        t := to_date(trim(input), 'YYYY-MM-DD');
        RETURN t;
    EXCEPTION WHEN others THEN NULL;
    END;

    BEGIN
        t := to_date(trim(input), 'DD/MM/YYYY');
        RETURN t;
    EXCEPTION WHEN others THEN NULL;
    END;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;


-- 💬 safe_numeric() removes symbols (like $, commas, etc.) and converts text to a number.
-- Returns NULL if the value isn’t valid.
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
-- 🧭 STEP 3: Build the main clean table (fact_employee_clean)
-- -------------------------------------------------------------
-- 💬 This is the main dataset used for analysis.
-- It cleans, standardizes, and enriches the employee data with useful derived fields.

DROP TABLE IF EXISTS hr.fact_employee_clean CASCADE;

CREATE TABLE hr.fact_employee_clean AS
SELECT
    -- 💬 Create a unique ID for each row
    row_number() OVER () AS surrogate_id,

    trim(employee_name) AS employee_name,
    trim(empid) AS employee_id,

    -- 💬 Keep the race/ethnicity description
    trim(racedesc) AS racedesc,

    -- 💬 Standardize gender into Male/Female/Unknown
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

    -- 💬 Engagement and satisfaction are scored from 0–5
    GREATEST(LEAST(hr.safe_numeric(engagementsurvey), 5), 0) AS engagement_survey,
    GREATEST(LEAST(hr.safe_numeric(empsatisfaction), 5), 0) AS emp_satisfaction,

    COALESCE(specialprojectscount, 0) AS special_projects_count,
    COALESCE(dayslatelast30, 0) AS days_late_last_30,
    COALESCE(absences, 0) AS absences,

    -- 💬 Simplify performance into categories (High / Medium / Low)
    CASE
        WHEN lower(coalesce(performancescore,'')) ~ 'exceed|excel' THEN 'High'
        WHEN lower(coalesce(performancescore,'')) ~ 'fully|meet|satisf' THEN 'Medium'
        WHEN lower(coalesce(performancescore,'')) ~ 'need|improv|poor' THEN 'Low'
        ELSE 'Unknown'
    END AS performance_category,

    -- 💬 Mark if the employee has left the company (TRUE/FALSE)
    CASE
        WHEN coalesce(termd, 0) = 1 THEN TRUE
        WHEN hr.safe_date(dateoftermination) IS NOT NULL THEN TRUE
        WHEN lower(coalesce(employmentstatus,'')) LIKE '%termin%' THEN TRUE
        ELSE FALSE
    END AS attrition_flag,

    -- 💬 Calculate length of employment in days and years
    CASE
        WHEN hr.safe_date(dateofhire) IS NULL THEN NULL
        ELSE (COALESCE(hr.safe_date(dateoftermination), CURRENT_DATE) - hr.safe_date(dateofhire))
    END AS tenure_days,

    CASE
        WHEN hr.safe_date(dateofhire) IS NULL THEN NULL
        ELSE ROUND(((COALESCE(hr.safe_date(dateoftermination), CURRENT_DATE) - hr.safe_date(dateofhire)) / 365.25)::numeric, 2)
    END AS tenure_years,

    -- 💬 Calculate the employee’s age when they were hired
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

-- 💬 Add a permanent unique ID and useful indexes for performance
ALTER TABLE hr.fact_employee_clean ADD COLUMN id BIGSERIAL PRIMARY KEY;

CREATE INDEX IF NOT EXISTS idx_hr_fact_empid ON hr.fact_employee_clean(employee_id);
CREATE INDEX IF NOT EXISTS idx_hr_fact_dept ON hr.fact_employee_clean(department);
CREATE INDEX IF NOT EXISTS idx_hr_fact_mgr ON hr.fact_employee_clean(manager_name);
CREATE INDEX IF NOT EXISTS idx_hr_fact_attrition ON hr.fact_employee_clean(attrition_flag);
CREATE INDEX IF NOT EXISTS idx_hr_fact_race ON hr.fact_employee_clean(racedesc);


-- -------------------------------------------------------------
-- 🧱 STEP 4: Create supporting “dimension” tables
-- -------------------------------------------------------------
-- 💬 These tables store unique categories (department, manager, position, race).
-- They help build relationships in Power BI or data warehouses.

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

-- 💬 Optional: A dimension for race/ethnicity
DROP TABLE IF EXISTS hr.dim_race CASCADE;
CREATE TABLE hr.dim_race AS
SELECT md5(coalesce(racedesc,'')) AS race_key, racedesc AS race_name
FROM (SELECT DISTINCT racedesc FROM hr.fact_employee_clean) t;
ALTER TABLE hr.dim_race ADD PRIMARY KEY (race_key);


-- -------------------------------------------------------------
-- 🚀 STEP 5: Next (optional) enhancements
-- -------------------------------------------------------------
-- 💬 Once this core model is built, you can add:
--   - A date dimension (for reporting by month/year)
--   - Monthly employee snapshots
--   - Metrics or performance summaries
--   - Views for Power BI dashboards
-- The data is now clean, consistent, and ready for business analysis.
