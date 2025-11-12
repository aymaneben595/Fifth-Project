# 📊 HR Attrition Analytics & Retention Pipeline

---

## 📘 Project Background

This project presents a **realistic end-to-end HR analytics workflow**, starting from **raw employee CSVs** and progressing all the way to **automated data processing** and **Power BI visualization**.

The goal is to mirror the day-to-day responsibilities of a **People Analytics** or **HR Analyst** — cleaning and transforming raw data in **PostgreSQL**, processing and enriching features in **Python**, and designing an **executive dashboard in Power BI** that helps analyse attrition, understand workforce dynamics, and improve retention.

### Key KPIs
- **Total Employees:** 298  
- **Overall Attrition %:** 33.44%  
- **Average Salary:** \$69.46K  
- **Average Tenure (Years):** 9.3  
- **Average Satisfaction:** 3.9  

The project revolves around two interactive dashboard pages:
1. **HR Overview:** Key KPIs, attrition trends, and workforce composition.  
2. **Manager Drill-Through:** Team-specific performance and retention metrics.

---

🔗 **Dataset Source:**  
[Human Resources Data Set – Kaggle](https://www.kaggle.com/datasets/rhuebner/human-resources-data-set?utm_source=chatgpt.com)

🔗 **SQL ETL Script:**  
[View ETL & Schema Creation (hr_data_pipeline.sql)](https://github.com/aymaneben595/Fifth-Project/blob/688230383bbdfffb39eeec0951069618a5c42c0e/VSCode%2C%20SQL%20%26%20Python/SQL/hr_data_pipeline.sql)

🐍 **Python Processing Script:**  
[View Processing & BI Export (hr_reporting_pipeline.py)](https://github.com/aymaneben595/Fifth-Project/blob/688230383bbdfffb39eeec0951069618a5c42c0e/VSCode%2C%20SQL%20%26%20Python/Python/hr_reporting_pipeline.py)

📊 **Power BI Dashboard:**  
[⬇️ Download Power BI Report (HR Analytics.pbix)](https://github.com/aymaneben595/Fifth-Project/raw/688230383bbdfffb39eeec0951069618a5c42c0e/Power%20Bi/Dashboard.pbix)

---

## 🚀 Workflow Overview

The pipeline follows a **three-stage structure**, mirroring a modern data analytics workflow.

### 1️⃣ SQL: ETL & Star Schema
- Created a dedicated `hr` schema in PostgreSQL.  
- Ingested the raw dataset (based on the Kaggle “Human Resources Data Set”) into a `raw_employees` table.  
- Cleaned and standardized messy inputs using `safe_date()` and `safe_numeric()` functions.  
- Built a normalized **star schema** with one central fact table:
  - `hr.fact_employee_clean`
  - and multiple dimension tables: `dim_department`, `dim_manager`, `dim_position`, `dim_race`.

### 2️⃣ Python: Feature Engineering & BI Exports
- Connected directly to PostgreSQL using **SQLAlchemy**.  
- Merged all fact and dimension tables into a single enriched **`df_analytics`** DataFrame.  
- Engineered key analytical features:
  - `tenure_days` and `tenure_bucket` (“<6 months”, “1–3 years”, etc.)  
  - `event_month` and `event_year` for time-series attrition analysis  
  - `attrition_flag` for active vs. terminated employees  
- Generated **two export layers**:
  - `/powerbi/` → Clean, normalized star schema for BI (`fact_hr_clean.csv`, `dim_departments.csv`, etc.)  
  - `/showcase/` → Wide, enriched datasets for portfolio and EDA (`hr_analytics_showcase.csv`, `monthly_summary_showcase.csv`).

### 3️⃣ Power BI: Interactive Visualization
- Modeled relationships between fact and dimension tables.  
- Built executive KPI cards, attrition trend charts, and workforce breakdowns.  
- Added a **drill-through** page for manager-level performance and retention insights.

---

## 🧩 Data Pipeline Summary

The dataset (derived from the Kaggle “Human Resources Data Set”) underwent a full transformation from a flat file to a robust, analytics-ready structure.

| Stage         | Description |
| ------------- | ---------- |
| **Ingestion** | Loaded CSV into `hr.raw_employees` |
| **Cleaning**  | Handled invalid dates, salaries, and text inconsistencies |
| **Transformation** | Standardized performance, gender, and department values |
| **Modeling**  | Created star schema for efficient BI integration |
| **Export**    | Generated showcase and Power BI datasets |

---

## 📈 Executive Summary

From a workforce of **298 employees**, the organization faces a notable **33.44% attrition rate**. The average tenure is **9.3 years**, with an average satisfaction score of **3.9 / 5**.

| Metric                         | Value |
| ------------------------------ | ----- |
| **Total Employees**            | 298   |
| **Overall Attrition %**        | 33.44%|
| **Average Salary**             | \$69.46K |
| **Average Tenure (Years)**     | 9.3   |
| **Average Satisfaction**       | 3.9   |
| **Workforce Diversity (White)**| 60%   |
| **Workforce Diversity (Black/Afr. Am.)** | 27% |
| **Workforce Diversity (Asian)**| 9%    |

<p align="center">
  <img src="Images/daash.PNG" alt="HR Overview Dashboard" width="800">
</p>

---

## 🔍 Insights Breakdown

### 🧭 Page 1 — HR Overview Dashboard
- **High Attrition:** 33.44% overall — significant retention risk. Spikes observed during 2013–2016.  
- **Workforce Composition:** 60% White, 27% Black/African American, 9% Asian.  
- **Performance:** 82% rated “Fully Meets”; 12% “High”; 6% “Low”.  
- **Tenure:** A large group (<6 months tenure) suggests onboarding or early-tenure attrition challenges.

### ⚙️ Page 2 — Manager Drill-Through (Example: Amy Dunn)
Focused managerial view with metrics per team.

| Metric             | Amy Dunn’s Team      |
| ------------------ | -------------------- |
| **Total Employees**| 19                   |
| **Attrition %**    | 68.42% ⚠️            |
| **Avg. Salary**    | \$57.80K             |
| **Avg. Satisfaction** | 3.8               |

<p align="center">
  <img src="Images/daash1.PNG" alt="Manager Drill-Through Dashboard" width="800">
</p>

---

## 💡 Business Recommendations

1. **Investigate High Attrition Drivers**  
   Analyse `TermReason` by department, manager, and tenure to identify root causes.

2. **Focus on New Hire Retention**  
   The high proportion of employees under 6 months indicates onboarding or culture-fit issues.

3. **Review Managerial Influence**  
   Amy Dunn’s team (68% attrition) is a key red-flag — consider leadership review or team restructuring.

4. **Evaluate Compensation vs. Attrition**  
   Lower salaries strongly correlate with higher turnover. Conduct a pay-equity study, especially for “Production Technician” roles.

---

## ⚙️ Assumptions & Notes

- Dataset sourced from the [Kaggle “Human Resources Data Set”] link above.
- Attrition is derived using `date_of_termination` (active employees have `NULL`).  
- SQL scripts generate a clean star schema consumed by Python.  
- Python exports both normalized BI tables and wide EDA-ready datasets.  
- Power BI relationships:  
  - `dim_departments[department_id] → fact_hr_clean[department_id]`  
  - `dim_positions[position_id] → fact_hr_clean[position_id]`  
  - `dim_managers[manager_id] → fact_hr_clean[manager_id]`  
  - `dim_race[race_id] → fact_hr_clean[race_id]`

---

<p align="center">
  <i>Created by Aïmane Benkhadda — End-to-End HR Analytics Project (PostgreSQL · Python · Power BI)</i><br>
  <a href="mailto:aymanebenkhadda5959@gmail.com">aymanebenkhadda5959@gmail.com</a>
</p>

---

✅ **In short:**  
This project combines **SQL-based ETL**, **Python-based feature engineering**, and **Power BI visualization** into one complete analytical solution — the kind of workflow a People Analytics professional would build to help a company understand its workforce and reduce attrition.
