This project builds an automated weather forecasting system using Open-Meteo API, Snowflake, and Apache Airflow. 
The system retrieves historical weather data for two cities, stores it in Snowflake, trains a forecasting model, and generates a 7-day temperature prediction daily.

## Cities
- San Jose, CA  
- Cupertino, CA  

## Architecture
Open-Meteo API  
→ Airflow ETL DAG  
→ Snowflake (Stage → Merge → Historical Table)  
→ Airflow Forecast DAG  
→ Snowflake ML Forecast  
→ Final UNION Output  

## Snowflake Objects
- `WEATHER_STAGE` – staging table  
- `WEATHER_PREDICTION_DAILY` – historical data  
- `WEATHER_FORECAST_DAILY` – forecast results  
- `WEATHER_PREDICTION_FINAL` – UNION of actual + forecast  

## Airflow DAGs
1. **weather_etl_openmeteo**
   - Runs daily
   - Fetches last 60 days of data
   - Loads into Snowflake using MERGE

2. **TrainPredict**
   - Runs after ETL
   - Trains Snowflake ML model
   - Forecasts next 7 days
   - Creates final UNION output

## Key Features
- Multi-city support using Airflow Variables  
- Idempotent pipeline using MERGE  
- Transaction control (BEGIN / COMMIT / ROLLBACK)  
- Automated scheduling using Airflow  

## Final Output
Forecast and historical data are combined using:

```sql
SELECT * FROM WEATHER_PREDICTION_FINAL;
