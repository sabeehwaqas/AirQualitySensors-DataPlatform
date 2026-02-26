# AirQualitySensors-DataPlatform

Air quality sensor streaming data platform with Kafka, Flink, Cassandra, Airflow &amp; FastAPI. A producer polls an air-quality API and publishes to Kafka topics per tenant. Flink ingests bronze data into Cassandra in parallel. Airflow runs silver batch pipelines with SLA configs.
