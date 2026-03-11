# AirQualitySensors-DataPlatform

Air quality sensor streaming data platform with Kafka, Cassandra, APScheduler, FastAPI. A producer polls an air-quality API and publishes to Kafka topics per tenant and ingests bronze data into Cassandra. Airflow runs silver batch pipelines with SLA configs.
