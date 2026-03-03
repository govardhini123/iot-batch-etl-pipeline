## Problem Statement

IoT devices generate large volumes of sensor telemetry data.  
This project demonstrates how raw device data can be transformed into an analytics-ready warehouse using dimensional modeling and batch ETL processing.

## Design Decisions

- Used star schema for analytical efficiency
- Dimensions loaded safely using ON CONFLICT
- Fact table refreshed for idempotent batch runs
- Aggregation layer built for reporting optimization
- SQLAlchemy 2.x compatible execution
