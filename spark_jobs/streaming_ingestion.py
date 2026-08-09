"""
GameGuard Lakehouse - Spark Streaming Job

Reads telemetry events from Kafka and writes to Iceberg bronze layer (raw_events).
Features:
- Micro-batch processing (5-10 seconds)
- Watermarking for event time handling
- Checkpointing for exactly-once semantics
- Basic threshold-based anomaly detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, watermark, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
KAFKA_TOPIC = "raw.telemetry"
CHECKPOINT_LOCATION = "/tmp/spark/checkpoints/raw_events"
ICEBERG_CATALOG = "gameguard"
BRONZE_TABLE = "bronze.raw_events"
ANOMALY_TABLE = "silver.anomaly_events"
BATCH_DURATION_SECONDS = 10

# Event schema matching the generator
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("player_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("position_x", DoubleType(), True),
    StructField("position_y", DoubleType(), True),
    StructField("position_z", DoubleType(), True),
    StructField("rotation_yaw", DoubleType(), True),
    StructField("rotation_pitch", DoubleType(), True),
    StructField("health", IntegerType(), True),
    StructField("ammo", IntegerType(), True),
    StructField("weapon_id", StringType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("is_alive", BooleanType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("damage_dealt", IntegerType(), True),
    StructField("damage_taken", IntegerType(), True),
    StructField("movement_speed", DoubleType(), True),
    StructField("reaction_time_ms", IntegerType(), True),
    StructField("accuracy", DoubleType(), True),
    StructField("headshot_ratio", DoubleType(), True),
    StructField("view_angle_change", DoubleType(), True),
    StructField("distance_traveled", DoubleType(), True),
    StructField("client_version", StringType(), True),
    StructField("region", StringType(), True),
])


def create_spark_session():
    """Create Spark session with Iceberg support."""
    spark = SparkSession.builder \
        .appName("GameGuard Raw Events Ingestion") \
        .config("spark.sql.catalog.gameguard", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gameguard.type", "rest") \
        .config("spark.sql.catalog.gameguard.uri", "http://iceberg-rest-catalog:8181") \
        .config("spark.sql.catalog.gameguard.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.gameguard.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.gameguard.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.gameguard.s3.access-key-id", "minioadmin") \
        .config("spark.sql.catalog.gameguard.s3.secret-access-key", "minioadmin") \
        .config("spark.sql.catalog.gameguard.s3.path-style-access", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_tables_exist(spark):
    """Create Iceberg tables if they don't exist."""
    # Create bronze schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.bronze")
    
    # Create silver schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.silver")
    
    # Create raw_events table (bronze layer)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{BRONZE_TABLE} (
            event_id STRING,
            event_time TIMESTAMP,
            event_received TIMESTAMP,
            player_id STRING,
            session_id STRING,
            match_id STRING,
            event_type STRING,
            position_x DOUBLE,
            position_y DOUBLE,
            position_z DOUBLE,
            rotation_yaw DOUBLE,
            rotation_pitch DOUBLE,
            health INT,
            ammo INT,
            weapon_id STRING,
            team_id INT,
            is_alive BOOLEAN,
            kills INT,
            deaths INT,
            assists INT,
            damage_dealt INT,
            damage_taken INT,
            movement_speed DOUBLE,
            reaction_time_ms INT,
            accuracy DOUBLE,
            headshot_ratio DOUBLE,
            view_angle_change DOUBLE,
            distance_traveled DOUBLE,
            client_version STRING,
            region STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        LOCATION 's3://iceberg-data/bronze/raw_events'
    """)
    
    # Create anomaly_events table (silver layer)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ANOMALY_TABLE} (
            event_id STRING,
            event_time TIMESTAMP,
            player_id STRING,
            session_id STRING,
            match_id STRING,
            anomaly_type STRING,
            anomaly_reason STRING,
            severity STRING,
            original_accuracy DOUBLE,
            original_reaction_time_ms INT,
            original_movement_speed DOUBLE,
            original_headshot_ratio DOUBLE,
            original_view_angle_change DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        LOCATION 's3://iceberg-data/silver/anomaly_events'
    """)
    
    print(f"Tables ensured: {BRONZE_TABLE}, {ANOMALY_TABLE}")


def detect_anomalies(df):
    """Apply threshold-based anomaly detection rules."""
    # Cheat detection thresholds
    anomalies = df.filter(
        (col("accuracy") > 0.85) |  # Too high accuracy
        (col("reaction_time_ms") < 100) |  # Superhuman reaction
        (col("movement_speed") > 15.0) |  # Speedhack
        (col("headshot_ratio") > 0.6) |  # Too many headshots
        (col("view_angle_change") > 90.0)  # Instant snap
    ).withColumn(
        "anomaly_type",
        # Classify anomaly type
        when(col("accuracy") > 0.85, "aimbot")
        .when(col("reaction_time_ms") < 100, "aimbot")
        .when(col("movement_speed") > 15.0, "speedhack")
        .when(col("headshot_ratio") > 0.6, "aimbot")
        .when(col("view_angle_change") > 90.0, "aimbot")
        .otherwise("unknown")
    ).withColumn(
        "anomaly_reason",
        # Detailed reason
        when(col("accuracy") > 0.85, f"accuracy={col('accuracy')}")
        .when(col("reaction_time_ms") < 100, f"reaction_time={col('reaction_time_ms')}ms")
        .when(col("movement_speed") > 15.0, f"speed={col('movement_speed')}m/s")
        .when(col("headshot_ratio") > 0.6, f"headshot_ratio={col('headshot_ratio')}")
        .when(col("view_angle_change") > 90.0, f"view_snap={col('view_angle_change')}deg")
        .otherwise("multiple indicators")
    ).withColumn(
        "severity",
        # Severity based on how extreme the values are
        when((col("accuracy") > 0.95) | (col("movement_speed") > 30.0), "critical")
        .when((col("accuracy") > 0.90) | (col("movement_speed") > 20.0), "high")
        .otherwise("medium")
    )
    
    return anomalies


def run_streaming_job():
    """Main streaming job."""
    print("Starting GameGuard streaming ingestion...")
    
    spark = create_spark_session()
    
    # Ensure target tables exist
    ensure_tables_exist(spark)
    
    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON payload
    parsed_df = raw_df.select(
        col("key").cast("string").alias("player_key"),
        from_json(col("value").cast("string"), event_schema).alias("event")
    ).select(
        col("event.*"),
        col("player_key")
    )
    
    # Add event_received timestamp and convert event_time
    enriched_df = parsed_df.withColumn(
        "event_received",
        current_timestamp()
    ).withColumn(
        "event_time_parsed",
        unix_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").cast("timestamp")
    ).withColumn(
        "event_time",
        col("event_time_parsed")
    ).drop("event_time_parsed", "player_key")
    
    # Apply watermark for late data handling (5 minute window)
    watermarked_df = watermark(enriched_df, "event_time", "5 minutes")
    
    # Detect anomalies
    anomaly_df = detect_anomalies(watermarked_df)
    
    # Write raw events to bronze layer
    raw_query = watermarked_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/raw") \
        .option("fanout-enabled", "true") \
        .trigger(processingTime=f"{BATCH_DURATION_SECONDS} seconds") \
        .toTable(f"{ICEBERG_CATALOG}.{BRONZE_TABLE}")
    
    # Write anomalies to silver layer
    anomaly_query = anomaly_df.select(
        col("event_id"),
        col("event_time"),
        col("player_id"),
        col("session_id"),
        col("match_id"),
        col("anomaly_type"),
        col("anomaly_reason"),
        col("severity"),
        col("accuracy").alias("original_accuracy"),
        col("reaction_time_ms").alias("original_reaction_time_ms"),
        col("movement_speed").alias("original_movement_speed"),
        col("headshot_ratio").alias("original_headshot_ratio"),
        col("view_angle_change").alias("original_view_angle_change")
    ).writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/anomaly") \
        .option("fanout-enabled", "true") \
        .trigger(processingTime=f"{BATCH_DURATION_SECONDS} seconds") \
        .toTable(f"{ICEBERG_CATALOG}.{ANOMALY_TABLE}")
    
    print(f"Streaming started. Writing to {BRONZE_TABLE} and {ANOMALY_TABLE}")
    print(f"Batch duration: {BATCH_DURATION_SECONDS} seconds")
    print("Press Ctrl+C to stop")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming_job()
