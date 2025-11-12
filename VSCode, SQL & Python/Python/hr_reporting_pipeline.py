"""
===============================================================
💼 HR Attrition: EDA + Modeling + Power BI Export Pipeline
Author: Aïmane (Refactored by Gemini)
Date: 2025 (Updated, Race Handling Fixed)

🧾 Description (for non-technical users):
This script connects to the HR PostgreSQL database, cleans and enriches employee data,
and then creates two main outputs:
1. 📊 /showcase/  → A wide table for analysis and data exploration.
2. 📈 /powerbi/   → Clean, structured datasets ready for Power BI dashboards.
===============================================================
"""

# =============================================================================
# 📦 0. Import Libraries
# =============================================================================
# 💬 These packages handle data, charts, and database connections.
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import os
from datetime import datetime


# =============================================================================
# ✅ 1. Connect to the Database
# =============================================================================
# 💬 We define the database connection details below.
#    These credentials let Python access the HR tables in PostgreSQL.
# ⚠️ In production, passwords should be stored securely (e.g., environment variables).
DB_NAME = "fdb"
DB_USER = "postgres"
DB_PASS = "Aymaneb595."
DB_HOST = "localhost"
DB_PORT = "5432"

try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("✅ Connected to PostgreSQL")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    exit()


# =============================================================================
# ✅ 2. Load the Raw Tables from PostgreSQL
# =============================================================================
# 💬 This function reads tables from the HR database into pandas DataFrames.
#    If a table is missing or fails to load, it safely returns an empty table.
def load_table(table_name):
    """Safely loads a table from the connected database."""
    print(f"📥 Loading: {table_name}...")
    try:
        return pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")
        return pd.DataFrame()

# 💬 Load all relevant HR tables created by the SQL pipeline.
fact = load_table("hr.fact_employee_clean")
dim_dept_raw = load_table("hr.dim_department")
dim_pos_raw = load_table("hr.dim_position")
dim_mgr_raw = load_table("hr.dim_manager")
dim_race_raw = load_table("hr.dim_race")

if fact.empty:
    print("❌ Fact table is empty. Pipeline cannot continue.")
    exit()


# =============================================================================
# ✅ 3. Prepare Power BI Dimension Tables (with Numeric IDs)
# =============================================================================
# 💬 Power BI prefers numeric IDs instead of text keys (like department names).
#    Here we assign simple numeric IDs for each dimension.

dim_departments = dim_dept_raw[['department']].drop_duplicates().reset_index(drop=True)
dim_departments['department_id'] = dim_departments.index + 1
dim_departments = dim_departments.rename(columns={'department': 'department_name'})
print(f"Created dim_departments with {len(dim_departments)} rows")

dim_positions = dim_pos_raw[['position']].drop_duplicates().reset_index(drop=True)
dim_positions['position_id'] = dim_positions.index + 1
dim_positions = dim_positions.rename(columns={'position': 'position_title'})
print(f"Created dim_positions with {len(dim_positions)} rows")

dim_managers = dim_mgr_raw[['manager_name']].drop_duplicates().reset_index(drop=True)
dim_managers['manager_id'] = dim_managers.index + 1
print(f"Created dim_managers with {len(dim_managers)} rows")

dim_race = dim_race_raw[['race_name']].drop_duplicates().reset_index(drop=True)
dim_race['race_id'] = dim_race.index + 1
print(f"Created dim_race with {len(dim_race)} rows")


# =============================================================================
# ✅ 4. Build an Enriched Analytics Table (EDA / Showcase)
# =============================================================================
# 💬 We combine (“join”) the employee fact table with dimension tables.
#    This creates one large, rich dataset for analysis or modeling.

df_analytics = (
    fact
    .merge(dim_departments, left_on="department", right_on="department_name", how="left")
    .merge(dim_positions, left_on="position", right_on="position_title", how="left")
    .merge(dim_managers, on="manager_name", how="left")
    .merge(dim_race, left_on="racedesc", right_on="race_name", how="left")
)
print("✅ Merged analytics table created")
print(f"💡 Columns available: {', '.join(df_analytics.columns.to_list()[:10])}...")


# =============================================================================
# ✅ 5. Feature Engineering (Creating New Insights)
# =============================================================================
# 💬 Here we calculate new useful columns:
#    - tenure (days worked)
#    - event date (hire or termination)
#    - month & year for trend analysis
#    - whether an employee left (attrition flag)

date_cols = ["date_of_birth", "date_of_hire", "date_of_termination"]
for col in date_cols:
    df_analytics[col] = pd.to_datetime(df_analytics[col], errors='coerce')

df_analytics["event_date"] = df_analytics["date_of_termination"].fillna(df_analytics["date_of_hire"])
df_analytics["event_year"] = df_analytics["event_date"].dt.year
df_analytics["event_month"] = df_analytics["event_date"].dt.to_period("M").astype(str)

df_analytics["tenure_days"] = (df_analytics["event_date"] - df_analytics["date_of_hire"]).dt.days
df_analytics["tenure_bucket"] = pd.cut(
    df_analytics["tenure_days"],
    bins=[-1, 180, 365, 1095, 99999],
    labels=["<6 months", "6-12 months", "1-3 years", "3+ years"]
)

df_analytics["is_terminated"] = df_analytics["attrition_flag"].astype(int)
print("✅ Feature engineering complete")


# =============================================================================
# ✅ 6. Organizational Attrition Summary (Monthly)
# =============================================================================
# 💬 This summarizes attrition per month across the organization:
#    - How many employees started
#    - How many left
#    - Monthly attrition % and 3-month rolling average

monthly_summary = (
    df_analytics.groupby(["event_year", "event_month"])
    .agg(
        employees_start=("employee_id", "nunique"),
        employees_left=("is_terminated", "sum"),
    )
    .reset_index()
)

monthly_summary["attrition_rate"] = np.where(
    monthly_summary["employees_start"] > 0,
    monthly_summary["employees_left"] / monthly_summary["employees_start"],
    0
)
monthly_summary["attrition_percent"] = monthly_summary["attrition_rate"] * 100

monthly_summary = monthly_summary.sort_values(by="event_month")
monthly_summary["attrition_rolling3m_pct"] = (
    monthly_summary["attrition_percent"].rolling(window=3, min_periods=1).mean()
)
print("✅ Monthly org-level attrition fact table calculated")


# =============================================================================
# ✅ 7. Export Showcase (EDA) Datasets
# =============================================================================
# 💬 We save ready-to-use CSVs for analysis or presentations.
#    These are the detailed datasets (not yet cleaned for Power BI).

os.makedirs("output/showcase", exist_ok=True)
df_analytics.to_csv("output/showcase/hr_analytics_showcase.csv", index=False)
monthly_summary.to_csv("output/showcase/monthly_summary_showcase.csv", index=False)
print("✅ Showcase datasets exported")


# =============================================================================
# ✅ 8. Export Power BI Datasets (Clean Star Schema)
# =============================================================================
# 💬 We now create final cleaned tables for Power BI:
#    - Clean dimension tables (no “Unknown” values)
#    - Fact table with proper foreign key IDs
#    - Monthly attrition summary fact

os.makedirs("output/powerbi", exist_ok=True)

# --- Clean dimensions ---
dim_departments_clean = dim_departments.replace("Unknown", np.nan).dropna()
dim_positions_clean = dim_positions.replace("Unknown", np.nan).dropna()
dim_managers_clean = dim_managers.replace("Unknown", np.nan).dropna()
dim_race_clean = dim_race.replace("Unknown", np.nan).dropna()

dim_departments_clean.to_csv("output/powerbi/dim_departments.csv", index=False)
dim_positions_clean.to_csv("output/powerbi/dim_positions.csv", index=False)
dim_managers_clean.to_csv("output/powerbi/dim_managers.csv", index=False)
dim_race_clean.to_csv("output/powerbi/dim_race.csv", index=False)
print("✅ Clean Dimension tables exported")


# --- Clean fact table ---
fact_hr_final = df_analytics.drop(columns=[
    'department',
    'position',
    'department_name',
    'position_title',
    'manager_name',
    'race_name'
], errors='ignore')

fact_hr_clean = fact_hr_final.replace("Unknown", np.nan)

# 💬 Remove employees with missing or invalid race values
if 'racedesc' in fact_hr_clean.columns:
    fact_hr_clean['racedesc'] = fact_hr_clean['racedesc'].replace("Unknown", np.nan)
    fact_hr_clean = fact_hr_clean.dropna(subset=['racedesc'])

# 💬 Drop any rows missing key information like ID, department, position, etc.
critical_cols = [
    'employee_id',
    'department_id',
    'position_id',
    'gender',
    'salary',
    'date_of_hire',
    'performance_category'
]
cols_to_check = [col for col in critical_cols if col in fact_hr_clean.columns]
fact_hr_clean = fact_hr_clean.dropna(subset=cols_to_check)

print(f"✅ Final Power BI fact table rows: {len(fact_hr_clean)}")
fact_hr_clean.to_csv("output/powerbi/fact_hr_clean.csv", index=False)
print("✅ Fact table 'fact_hr_clean.csv' exported")


# --- Monthly attrition summary fact ---
fact_attrition_columns = [
    'event_month',
    'employees_start',
    'employees_left',
    'attrition_rate',
    'attrition_percent',
    'attrition_rolling3m_pct'
]
fact_attrition_clean = monthly_summary[fact_attrition_columns].rename(
    columns={'event_month': 'month'}
).dropna()

fact_attrition_clean.to_csv("output/powerbi/fact_attrition_monthly.csv", index=False)
print("✅ Fact table 'fact_attrition_monthly.csv' exported")


# =============================================================================
# ✅ 9. Plot Monthly Attrition Trend
# =============================================================================
# 💬 This creates and saves a line chart showing monthly attrition trends
#    and the 3-month rolling average — useful for HR presentations.

plot_data = monthly_summary.sort_values(by="event_month")
plt.figure(figsize=(14, 7))
ax = plt.gca()

plt.plot(
    plot_data["event_month"],
    plot_data["attrition_percent"],
    marker='o', linestyle='-', alpha=0.5, label="Monthly Attrition (%)"
)
plt.plot(
    plot_data["event_month"],
    plot_data["attrition_rolling3m_pct"],
    linestyle='-', linewidth=2, color='red', label="3-Month Rolling Avg"
)

plt.title("Overall Monthly Attrition Trend", fontsize=16)
plt.xlabel("Month", fontsize=12)
plt.ylabel("Attrition %", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
ax.xaxis.set_major_locator(MaxNLocator(nbins=12))
plt.xticks(rotation=30, ha='right')
plt.tight_layout()
plt.savefig("output/showcase/org_attrition_trend.png")
print("📊 Chart saved: output/showcase/org_attrition_trend.png")


# =============================================================================
# ✅ 10. Final Output Summary
# =============================================================================
# 💬 Everything is now exported. The pipeline is complete and ready for use in BI dashboards.

print("\n✅ HR Attrition Pipeline Completed Successfully!")
print("✅ All datasets exported to /output/showcase/ and /output/powerbi/")
print("💡 Power BI Relationships:")
print("   - dim_departments[department_id] -> fact_hr_clean[department_id]")
print("   - dim_positions[position_id]     -> fact_hr_clean[position_id]")
print("   - dim_managers[manager_id]       -> fact_hr_clean[manager_id]")
print("   - dim_race[race_id]              -> fact_hr_clean[race_id]")
print("   (fact_attrition_monthly = org-level summary table)\n")
