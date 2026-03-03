import pandas as pd
import random
from datetime import datetime, timedelta

data = []

start_date = datetime(2026, 1, 1)

for i in range(1000):
    device_id = f"MTR_{random.randint(100, 110)}"

    # Spread across 10 days
    random_days = random.randint(0, 9)
    random_minutes = random.randint(0, 1440)

    timestamp = start_date + timedelta(days=random_days, minutes=random_minutes)

    temperature = round(random.uniform(60, 100), 2)
    vibration = round(random.uniform(0.01, 0.05), 3)

    if temperature > 90:
        risk_level = "HIGH"
    elif temperature > 75:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    data.append([
        device_id,
        timestamp,
        temperature,
        vibration,
        risk_level
    ])

df = pd.DataFrame(data, columns=[
    "device_id",
    "timestamp",
    "temperature",
    "vibration",
    "risk_level"
])

df.to_csv("data/iot_device_data.csv", index=False)

print("Multi-day IoT data generated successfully!")