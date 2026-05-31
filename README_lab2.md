# BigDataSpark — лабораторная работа №2

## Что есть в проекте
- 10 CSV-файлов `mock_data`
- `docker-compose.yml` для PostgreSQL, Spark и ClickHouse
- `spark_scripts/star.py` — построение модели звезда в PostgreSQL
- `spark_scripts/clickhouse_reports.py` — построение витрин в ClickHouse

## Как запустить

### 1. Поднять контейнеры
```powershell
docker compose up -d
```

### 2. Запустить ETL в модель звезда
```powershell
docker exec -it bigdata_spark spark-submit --jars /home/jovyan/work/drivers/postgresql-42.7.11.jar /home/jovyan/work/spark_scripts/star.py
```

### 3. Запустить построение витрин ClickHouse
```powershell
docker exec -it bigdata_spark spark-submit --jars /home/jovyan/work/drivers/postgresql-42.7.11.jar,/home/jovyan/work/drivers/clickhouse-jdbc-0.9.7-all.jar /home/jovyan/work/spark_scripts/clickhouse_reports.py
```

## Что реализовано
- загрузка исходных данных в PostgreSQL
- модель звезда в PostgreSQL
- 6 витрин в ClickHouse
