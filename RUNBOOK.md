# GameGuard Lakehouse - Runbook

## Быстрый старт (5 минут)

### Шаг 1: Запуск окружения

```bash
cd /workspace
make up
```

Ожидайте 30-60 секунд для полного запуска сервисов.

**Проверка:**
```bash
docker ps | grep gameguard
# Должны быть видны: minio, kafka, iceberg-rest, spark-master, trino
```

### Шаг 2: Генерация тестовых данных

В новом терминале:

```bash
make generate-data
```

Генератор будет работать 60 секунд, создавая ~300 событий.

**Что происходит:**
- 10 нормальных игроков генерируют реалистичные события
- 2 читера (aimbot + speedhack) генерируют аномальные паттерны
- События отправляются в Kafka topic `raw.telemetry`

### Шаг 3: Запуск streaming-обработки

В новом терминале:

```bash
make streaming
```

**Что происходит:**
- Spark читает из Kafka каждые 10 секунд
- Создаётся таблица `gameguard.bronze.raw_events`
- Аномалии пишутся в `gameguard.silver.anomaly_events`

### Шаг 4: Проверка через SQL

```bash
make query-raw
```

Ожидаемый результат:
```
 event_id | event_time | player_id | accuracy | movement_speed | reaction_time_ms
----------+------------+-----------+----------+----------------+------------------
 ...      | ...        | player_...| 0.35     | 5.2            | 245
 ...      | ...        | player_...| 0.92     | 25.3           | 85    <-- читер
```

### Шаг 5: Запуск batch-обработки

В новом терминале (пока streaming работает):

```bash
make batch
```

**Что происходит:**
- Агрегируются дневные признаки по игрокам
- Рассчитывается risk_score
- Результаты в `gameguard.silver.player_features_daily`

### Шаг 6: Анализ результатов

```bash
make query-anomaly
make query-features
```

Или подключитесь к Trino CLI:
```bash
make sql-check
```

Пример запроса для поиска читеров:
```sql
SELECT 
    player_id,
    avg_accuracy,
    max_movement_speed,
    total_anomalies,
    risk_score
FROM gameguard.silver.player_features_daily
ORDER BY risk_score DESC
LIMIT 5;
```

## Остановка

```bash
# Остановить streaming job (Ctrl+C)
# Остановить все сервисы
make down
```

## Troubleshooting

### Сервисы не стартуют

```bash
make logs
make clean
make up
```

### Нет данных в таблицах

1. Проверьте Kafka:
```bash
docker exec gameguard-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic raw.telemetry
```

2. Проверьте Spark логи:
```bash
make logs-spark
```

3. Перезапустите streaming:
```bash
make down
rm -rf /tmp/spark/checkpoints
make up
make generate-data
make streaming
```

### Trino не видит таблицы

```sql
SHOW SCHEMAS FROM gameguard;
SHOW TABLES FROM gameguard.bronze;
```

Если таблиц нет — дождитесь завершения первого батча Spark.

## Архитектура данных

```
Kafka (raw.telemetry)
    ↓ (Spark Streaming, 10 sec batch)
Iceberg bronze.raw_events (partitioned by day)
    ↓ (Spark Streaming filters)
Iceberg silver.anomaly_events
    
Iceberg bronze.raw_events
    ↓ (Daily batch job)
Iceberg silver.player_features_daily (partitioned by date)
```

## Команды для ежедневного использования

```bash
# Старт
make up

# Данные
make generate-data

# Обработка
make streaming  # в фоне
make batch      # раз в день

# Запросы
make query-raw
make query-anomaly  
make query-features
make sql-check

# Стоп
make down
```

## Доступ к UI

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Spark UI**: http://localhost:8080
- **Trino**: http://localhost:8088

---

**Время выполнения MVP:** ~5 минут от `make up` до первых SQL-результатов.
