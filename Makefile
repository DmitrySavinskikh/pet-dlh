# GameGuard Lakehouse - Makefile

.PHONY: help up down restart logs clean setup generate-data streaming batch sql-check all

# Default target
help:
	@echo "GameGuard Lakehouse - Available Commands"
	@echo "=========================================="
	@echo ""
	@echo "Environment Management:"
	@echo "  make up            - Start all services (MinIO, Kafka, Iceberg, Spark, Trino)"
	@echo "  make down          - Stop all services"
	@echo "  make restart       - Restart all services"
	@echo "  make logs          - View logs from all services"
	@echo "  make logs-<svc>    - View logs from specific service (minio, kafka, spark, trino, iceberg)"
	@echo ""
	@echo "Setup & Data:"
	@echo "  make setup         - Initialize environment (create buckets, topics)"
	@echo "  make generate-data - Run telemetry generator (sends data to Kafka)"
	@echo ""
	@echo "Processing Jobs:"
	@echo "  make streaming     - Start Spark streaming job (Kafka -> Iceberg)"
	@echo "  make batch         - Run daily features batch job"
	@echo ""
	@echo "Querying:"
	@echo "  make sql-check     - Connect to Trino CLI for SQL queries"
	@echo "  make query-raw     - Query raw_events table"
	@echo "  make query-anomaly - Query anomaly_events table"
	@echo "  make query-features- Query player_features_daily table"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         - Remove all containers, volumes, and checkpoints"
	@echo "  make clean-data    - Remove only data (keep containers)"
	@echo ""
	@echo "Full Workflow:"
	@echo "  make all           - Up + Generate Data + Streaming (manual stop) + Batch"
	@echo ""

# Environment management
up:
	@echo "Starting GameGuard Lakehouse services..."
	cd docker && docker-compose up -d
	@echo "Services starting. Wait ~30 seconds for all components to be healthy."
	@echo "Check status with: make logs"

down:
	@echo "Stopping GameGuard Lakehouse services..."
	cd docker && docker-compose down

restart:
	@echo "Restarting GameGuard Lakehouse services..."
	cd docker && docker-compose restart

logs:
	cd docker && docker-compose logs -f

logs-minio:
	cd docker && docker-compose logs -f minio

logs-kafka:
	cd docker && docker-compose logs -f kafka

logs-spark:
	cd docker && docker-compose logs -f spark-master

logs-trino:
	cd docker && docker-compose logs -f trino

logs-iceberg:
	cd docker && docker-compose logs -f iceberg-rest-catalog

# Setup
setup:
	@echo "Setting up GameGuard Lakehouse environment..."
	@echo "Waiting for services to be ready..."
	sleep 15
	@echo "Checking MinIO buckets..."
	docker exec gameguard-minio-init mc ls myminio || echo "Buckets will be created on startup"
	@echo "Checking Kafka topics..."
	docker exec gameguard-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list || echo "Topics will be created on startup"
	@echo "Setup complete!"

# Data generation
generate-data:
	@echo "Generating synthetic telemetry data..."
	@echo "Make sure Kafka is running: make up"
	pip install kafka-python --quiet 2>/dev/null || true
	python scripts/telemetry_generator.py \
		--kafka-servers localhost:9092 \
		--topic raw.telemetry \
		--normal-players 10 \
		--cheaters 2 \
		--events-per-second 5 \
		--duration 60

# Streaming job
streaming:
	@echo "Starting Spark streaming ingestion job..."
	@echo "This will run continuously. Press Ctrl+C to stop."
	docker exec -it gameguard-spark-master \
		spark-submit --master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
		--conf spark.sql.catalog.gameguard=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.gameguard.type=rest \
		--conf spark.sql.catalog.gameguard.uri=http://iceberg-rest-catalog:8181 \
		--conf spark.sql.catalog.gameguard.warehouse=s3://warehouse/ \
		--conf spark.sql.catalog.gameguard.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
		--conf spark.sql.catalog.gameguard.s3.endpoint=http://minio:9000 \
		--conf spark.sql.catalog.gameguard.s3.access-key-id=minioadmin \
		--conf spark.sql.catalog.gameguard.s3.secret-access-key=minioadmin \
		--conf spark.sql.catalog.gameguard.s3.path-style-access=true \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		/opt/spark/jobs/streaming_ingestion.py

# Batch job
batch:
	@echo "Running daily features batch job..."
	docker exec -it gameguard-spark-master \
		spark-submit --master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
		--conf spark.sql.catalog.gameguard=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.gameguard.type=rest \
		--conf spark.sql.catalog.gameguard.uri=http://iceberg-rest-catalog:8181 \
		--conf spark.sql.catalog.gameguard.warehouse=s3://warehouse/ \
		--conf spark.sql.catalog.gameguard.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
		--conf spark.sql.catalog.gameguard.s3.endpoint=http://minio:9000 \
		--conf spark.sql.catalog.gameguard.s3.access-key-id=minioadmin \
		--conf spark.sql.catalog.gameguard.s3.secret-access-key=minioadmin \
		--conf spark.sql.catalog.gameguard.s3.path-style-access=true \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		/opt/spark/jobs/daily_features.py

# SQL queries via Trino
sql-check:
	@echo "Connecting to Trino CLI..."
	@echo "Use 'SHOW TABLES FROM gameguard.bronze;' to see tables"
	@echo "Use 'SELECT * FROM gameguard.bronze.raw_events LIMIT 10;' to query data"
	@echo "Type 'exit' to quit"
	docker exec -it gameguard-trino trino --server http://localhost:8088

query-raw:
	@echo "Querying raw_events table (last 10 records)..."
	docker exec gameguard-trino trino --server http://localhost:8088 \
		--execute "SELECT event_id, event_time, player_id, accuracy, movement_speed, reaction_time_ms FROM gameguard.bronze.raw_events ORDER BY event_time DESC LIMIT 10"

query-anomaly:
	@echo "Querying anomaly_events table..."
	docker exec gameguard-trino trino --server http://localhost:8088 \
		--execute "SELECT event_id, event_time, player_id, anomaly_type, severity, anomaly_reason FROM gameguard.silver.anomaly_events ORDER BY event_time DESC LIMIT 20"

query-features:
	@echo "Querying player_features_daily table..."
	docker exec gameguard-trino trino --server http://localhost:8088 \
		--execute "SELECT feature_date, player_id, total_kills, total_deaths, kd_ratio, avg_accuracy, risk_score FROM gameguard.silver.player_features_daily ORDER BY feature_date DESC, risk_score DESC LIMIT 20"

# Cleanup
clean:
	@echo "Cleaning up all containers, volumes, and checkpoints..."
	cd docker && docker-compose down -v
	rm -rf /tmp/spark/checkpoints
	@echo "Cleanup complete!"

clean-data:
	@echo "Removing all data but keeping containers..."
	cd docker && docker-compose down
	docker volume rm docker_minio_data docker_kafka_data 2>/dev/null || true
	rm -rf /tmp/spark/checkpoints
	@echo "Data cleanup complete! Run 'make up' to restart."

# Full workflow demo
all:
	@echo "============================================"
	@echo "GameGuard Lakehouse - Full Demo Workflow"
	@echo "============================================"
	@echo ""
	@echo "Step 1: Starting services..."
	make up
	@echo ""
	@echo "Step 2: Waiting for services to be ready (30 seconds)..."
	sleep 30
	@echo ""
	@echo "Step 3: Generating synthetic telemetry data..."
	make generate-data
	@echo ""
	@echo "Step 4: Starting streaming job (runs in background)..."
	@echo "NOTE: In a real scenario, this would run continuously."
	@echo "For demo, we'll submit it and check results after."
	@echo ""
	@echo "Step 5: Check Trino for available tables:"
	make sql-check
	@echo ""
	@echo "Demo complete! Use the following commands:"
	@echo "  - make query-raw      : See raw events"
	@echo "  - make query-anomaly  : See detected anomalies"
	@echo "  - make query-features : See player features"
	@echo "  - make down           : Stop all services"
