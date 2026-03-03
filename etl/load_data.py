import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# Database Connection
# -------------------------
username = "postgres"
password = "postgres123"
host = "localhost"
port = "5432"
database = "iot_analytics"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)

# -------------------------
# Load CSV
# -------------------------
df = pd.read_csv("data/iot_device_data.csv")

df["timestamp"] = pd.to_datetime(df["timestamp"])
df["full_date"] = df["timestamp"].dt.date

# -------------------------
# Load dim_device (Safe)
# -------------------------
unique_devices = df["device_id"].drop_duplicates()

with engine.begin() as conn:
    for device in unique_devices:
        conn.execute(
            text("""
                INSERT INTO dim_device (device_id)
                VALUES (:device_id)
                ON CONFLICT (device_id) DO NOTHING;
            """),
            {"device_id": device}
        )

# -------------------------
# Load dim_date (Safe)
# -------------------------
unique_dates = df["full_date"].drop_duplicates()

with engine.begin() as conn:
    for full_date in unique_dates:
        conn.execute(
            text("""
                INSERT INTO dim_date (full_date, year, month, day)
                VALUES (:full_date, :year, :month, :day)
                ON CONFLICT (full_date) DO NOTHING;
            """),
            {
                "full_date": full_date,
                "year": full_date.year,
                "month": full_date.month,
                "day": full_date.day
            }
        )

# -------------------------
# Refresh fact table
# -------------------------
with engine.begin() as conn:
    conn.execute(text("TRUNCATE fact_device_readings RESTART IDENTITY;"))

# -------------------------
# Load fact table
# -------------------------
date_df = pd.read_sql("SELECT * FROM dim_date", engine)

df = df.merge(date_df, on="full_date", how="left")

fact_data = df[[
    "device_id",
    "date_id",
    "temperature",
    "vibration",
    "risk_level"
]]

fact_data.to_sql(
    "fact_device_readings",
    engine,
    if_exists="append",
    index=False
)

print("ETL Process Completed Successfully!")

# -------------------------
# Build Daily Aggregation
# -------------------------

with engine.begin() as conn:
    conn.execute(text("TRUNCATE agg_daily_device_summary;"))

aggregation_query = """
INSERT INTO agg_daily_device_summary (
    device_id,
    date_id,
    avg_temperature,
    avg_vibration,
    high_risk_count
)
SELECT
    device_id,
    date_id,
    AVG(temperature) AS avg_temperature,
    AVG(vibration) AS avg_vibration,
    COUNT(*) FILTER (WHERE risk_level = 'HIGH') AS high_risk_count
FROM fact_device_readings
GROUP BY device_id, date_id;
"""

with engine.begin() as conn:
    conn.execute(text(aggregation_query))

print("Daily Aggregation Completed Successfully!")