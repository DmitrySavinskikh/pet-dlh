# GameGuard Lakehouse

**Платформа телеметрии и near-real-time обнаружения читеров для PC-игр**

## Описание

GameGuard Lakehouse — это платформа для сбора игровой телеметрии, обработки в near-real-time и обнаружения аномалий поведения игроков. Система построена на архитектуре Data Lakehouse с использованием Apache Iceberg, MinIO, Kafka и Trino.

### Ключевые возможности MVP

- ✅ Синтетический генератор телеметрии (нормальные игроки + читеры)
- ✅ Единая схема событий с игровыми метриками
- ✅ Отправка событий в Kafka (`raw.telemetry`)
- ✅ NRT-загрузка в Iceberg через Spark Structured Streaming (микробатчи 10 сек)
- ✅ Watermarking и checkpointing для надёжной обработки
- ✅ Пороговые правила обнаружения аномалий (aimbot, speedhack)
- ✅ Ежедневный расчёт признаков игроков (`player_features_daily`)
- ✅ SQL-доступ через Trino ко всем таблицам

## Архитектура

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│   Telemetry     │────▶│    Kafka     │────▶│  Spark Streaming    │
│   Generator     │     │ raw.telemetry│    │  (NRT Ingestion)    │
│   (Python)      │     │              │    │                     │
└─────────────────┘     └──────────────┘     └──────────┬──────────┘
                                                        │
                          ┌─────────────────────────────┼─────────────────────────────┐
                          │                             │                             │
                          ▼                             ▼                             │
                   ┌──────────────┐            ┌──────────────┐                       │
                   │   Iceberg    │            │   Iceberg    │                       │
                   │   bronze/    │            │   silver/    │◀──────────────────────┘
                   │ raw_events   │            │anomaly_events│
                   └──────┬───────┘            └──────┬───────┘
                          │                           │
                          └──────────────┬────────────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │    Trino     │
                                  │  (SQL Access)│
                                  └──────┬───────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │ ML Engineers │
                                  │  Analysts    │
                                  └──────────────┘
```

## Технологический стек

| Компонент | Технология | Назначение |
|-----------|------------|------------|
| Генерация данных | Python 3.11+ | Синтетическая телеметрия |
| Очередь событий | Apache Kafka 3.7 | Буфер событий |
| Обработка потока | Spark 3.5.1 | NRT ingestion в Iceberg |
| Объектное хранилище | MinIO | S3-совместимое хранение |
| Табличный формат | Apache Iceberg 1.4.3 | ACID таблицы |
| Каталог метаданных | Iceberg REST Catalog | Управление таблицами |
| SQL-движок | Trino 440 | Аналитические запросы |

## Быстрый старт

### Предварительные требования

- Docker и Docker Compose
- Python 3.11+ (для генератора данных)
- Make (опционально, для удобства)

### 1. Запуск окружения

```bash
# Запустить все сервисы
make up

# Или через docker-compose напрямую
cd docker && docker-compose up -d
```

Ожидайте ~30 секунд для полного запуска всех компонентов.

Проверить статус:
```bash
make logs
```

### 2. Генерация тестовых данных

```bash
# Сгенерировать телеметрию (60 секунд, 5 событий/сек)
make generate-data
```

Генератор создаст:
- 10 нормальных игроков
- 2 читера (aimbot, speedhack)
- ~300 событий в Kafka

### 3. Запуск streaming-обработки

```bash
# Запустить Spark streaming job
make streaming
```

Job будет:
- Читать события из Kafka каждые 10 секунд
- Писать в `gameguard.bronze.raw_events`
- Детектировать аномалии и писать в `gameguard.silver.anomaly_events`

**Остановить:** `Ctrl+C`

### 4. Запуск batch-обработки (признаки игроков)

В отдельном терминале:

```bash
make batch
```

### 5. Проверка результатов через SQL

```bash
# Подключиться к Trino CLI
make sql-check

# Или выполнить готовые запросы:
make query-raw        # Последние сырые события
make query-anomaly    # Обнаруженные аномалии
make query-features   # Признаки игроков
```

Примеры SQL-запросов:

```sql
-- Посмотреть таблицы
SHOW TABLES FROM gameguard.bronze;
SHOW TABLES FROM gameguard.silver;

-- Найти читеров по аномалиям
SELECT 
    player_id,
    anomaly_type,
    severity,
    anomaly_reason,
    event_time
FROM gameguard.silver.anomaly_events
ORDER BY event_time DESC
LIMIT 20;

-- Топ игроков по risk_score
SELECT 
    player_id,
    total_kills,
    total_deaths,
    kd_ratio,
    avg_accuracy,
    risk_score
FROM gameguard.silver.player_features_daily
ORDER BY risk_score DESC
LIMIT 10;

-- Статистика по таблице сырых событий
SELECT 
    count(*) as total_events,
    count(distinct player_id) as unique_players,
    count(distinct match_id) as unique_matches,
    date(min(event_time)) as earliest_event,
    date(max(event_time)) as latest_event
FROM gameguard.bronze.raw_events;
```

## Структура проекта

```
gameguard-lakehouse/
├── docker/
│   ├── docker-compose.yml       # Оркестрация сервисов
│   ├── .env                     # Переменные окружения
│   └── trino/
│       └── catalog/             # Конфигурация каталогов Trino
│           ├── gameguard.properties
│           └── kafka.properties
├── scripts/
│   └── telemetry_generator.py   # Генератор синтетических данных
├── spark_jobs/
│   ├── streaming_ingestion.py   # NRT обработка Kafka -> Iceberg
│   └── daily_features.py        # Batch расчёт признаков
├── Makefile                     # Команды управления
└── README.md                    # Этот файл
```

## Схема данных

### Bronze Layer: `raw_events`

| Поле | Тип | Описание |
|------|-----|----------|
| event_id | STRING | Уникальный ID события |
| event_time | TIMESTAMP | Время события (от клиента) |
| event_received | TIMESTAMP | Время получения (от сервера) |
| player_id | STRING | ID игрока |
| session_id | STRING | ID сессии |
| match_id | STRING | ID матча |
| event_type | STRING | Тип события |
| position_x/y/z | DOUBLE | Координаты |
| accuracy | DOUBLE | Точность стрельбы (0-1) |
| reaction_time_ms | INT | Время реакции (мс) |
| movement_speed | DOUBLE | Скорость перемещения (м/с) |
| headshot_ratio | DOUBLE | Процент хедшотов (0-1) |
| view_angle_change | DOUBLE | Изменение угла обзора (градусы) |
| kills/deaths/assists | INT | Статистика |
| ... | ... | ... |

### Silver Layer: `anomaly_events`

| Поле | Тип | Описание |
|------|-----|----------|
| event_id | STRING | ID события |
| player_id | STRING | ID игрока |
| anomaly_type | STRING | Тип аномалии (aimbot/speedhack) |
| severity | STRING | Критичность (low/medium/high/critical) |
| anomaly_reason | STRING | Причина детекции |
| original_* | DOUBLE/INT | Исходные значения метрик |

### Silver Layer: `player_features_daily`

| Поле | Тип | Описание |
|------|-----|----------|
| feature_date | DATE | Дата агрегации |
| player_id | STRING | ID игрока |
| total_sessions | INT | Кол-во сессий |
| total_kills/deaths | INT | Статистика |
| kd_ratio | DOUBLE | K/D ratio |
| avg_accuracy | DOUBLE | Средняя точность |
| avg_reaction_time_ms | DOUBLE | Среднее время реакции |
| total_anomalies | INT | Кол-во аномалий за день |
| risk_score | DOUBLE | Общий риск (0-1+) |

## Правила обнаружения аномалий (MVP)

Система детектирует читеров по пороговым значениям:

| Аномалия | Правило | Порог |
|----------|---------|-------|
| **Aimbot** | Точность > 85% | `accuracy > 0.85` |
| **Aimbot** | Время реакции < 100мс | `reaction_time_ms < 100` |
| **Aimbot** | Headshot ratio > 60% | `headshot_ratio > 0.6` |
| **Aimbot** | Мгновенный поворот > 90° | `view_angle_change > 90` |
| **Speedhack** | Скорость > 15 м/с | `movement_speed > 15.0` |

**Risk Score** рассчитывается как взвешенная сумма отклонений от нормального поведения.

## Полезные команды

```bash
# Просмотр логов
make logs
make logs-kafka
make logs-spark

# Перезапуск сервисов
make restart

# Остановка
make down

# Полная очистка (данные + контейнеры)
make clean

# Очистка только данных
make clean-data

# Справка по командам
make help
```

## Доступ к сервисам

| Сервис | URL | Логин/Пароль |
|--------|-----|--------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:9092 | - |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Spark UI | http://localhost:8080 | - |
| Trino | http://localhost:8088 | - |

## Расширение (Nice-to-have)

Следующие функции запланированы после MVP:

- [ ] Статистические аномалии (z-score, EWMA)
- [ ] Алерты в Telegram/webhook
- [ ] SCD Type 2 для истории профилей игроков
- [ ] Schema Registry (Avro/Protobuf)
- [ ] Оркестрация через Airflow/Dagster
- [ ] Мониторинг (Prometheus + Grafana)
- [ ] ML-скоринг (загрузка моделей)
- [ ] Feature Store

## Troubleshooting

### Сервисы не запускаются

```bash
# Проверить логи
make logs

# Пересоздать контейнеры
make clean
make up
```

### Kafka недоступна

```bash
# Проверить статус
docker exec gameguard-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Пересоздать топики
docker-compose -f docker/docker-compose.yml up -d kafka-init
```

### Spark job падает

Проверить логи Spark:
```bash
make logs-spark
```

Убедиться, что Iceberg REST Catalog доступен:
```bash
curl http://localhost:8181/management/v1/ping
```

### Trino не видит таблицы

Обновить каталог:
```sql
SHOW SCHEMAS FROM gameguard;
```

Проверить наличие таблиц:
```sql
SHOW TABLES FROM gameguard.bronze;
```

## Лицензия

MIT License

---

**GameGuard Lakehouse** — MVP платформы для обнаружения читеров в near-real-time.
