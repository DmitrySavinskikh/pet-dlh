"""
GameGuard Lakehouse - Daily Batch Job for Player Features

Calculates daily aggregated features for each player and stores in silver layer.
Features include:
- Total kills, deaths, assists
- Average accuracy, reaction time, movement speed
- Headshot ratio
- Session counts
- Anomaly counts
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, date_trunc, lit, current_timestamp
from pyspark.sql.types import TimestampType
import sys

# Configuration
ICEBERG_CATALOG = "gameguard"
BRONZE_TABLE = "bronze.raw_events"
ANOMALY_TABLE = "silver.anomaly_events"
FEATURES_TABLE = "silver.player_features_daily"


def create_spark_session():
    """Create Spark session with Iceberg support."""
    spark = SparkSession.builder \
        .appName("GameGuard Daily Features") \
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
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_features_table(spark):
    """Create player_features_daily table if it doesn't exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.silver")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{FEATURES_TABLE} (
            feature_date DATE,
            player_id STRING,
            total_sessions INT,
            total_matches INT,
            total_events INT,
            total_kills INT,
            total_deaths INT,
            total_assists INT,
            kd_ratio DOUBLE,
            avg_accuracy DOUBLE,
            avg_reaction_time_ms DOUBLE,
            avg_movement_speed DOUBLE,
            avg_headshot_ratio DOUBLE,
            max_accuracy DOUBLE,
            min_reaction_time_ms INT,
            max_movement_speed DOUBLE,
            total_anomalies INT,
            anomaly_types ARRAY<STRING>,
            risk_score DOUBLE,
            last_updated TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (feature_date)
        LOCATION 's3://iceberg-data/silver/player_features_daily'
    """)
    
    print(f"Table ensured: {FEATURES_TABLE}")


def calculate_daily_features(spark, target_date=None):
    """Calculate daily features for all players."""
    
    # Determine target date (yesterday by default)
    if target_date:
        date_filter = f"date(event_time) = DATE '{target_date}'"
        feature_date = target_date
    else:
        # Default to yesterday
        spark.sql("SET spark.sql.session.timeZone=UTC")
        date_filter = "date(event_time) = date_sub(current_date(), 1)"
        feature_date = "date_sub(current_date(), 1)"
    
    print(f"Calculating features for date: {feature_date}")
    
    # Read raw events
    raw_events = spark.table(f"{ICEBERG_CATALOG}.{BRONZE_TABLE}").filter(date_filter)
    
    if raw_events.count() == 0:
        print(f"No events found for date {feature_date}")
        return
    
    # Aggregate player features
    player_features = raw_events.groupBy(
        date_trunc("day", col("event_time")).alias("feature_date"),
        col("player_id")
    ).agg(
        count(distinct(col("session_id"))).alias("total_sessions"),
        count(distinct(col("match_id"))).alias("total_matches"),
        count(col("event_id")).alias("total_events"),
        sum(col("kills")).alias("total_kills"),
        sum(col("deaths")).alias("total_deaths"),
        sum(col("assists")).alias("total_assists"),
        avg(col("accuracy")).alias("avg_accuracy"),
        avg(col("reaction_time_ms")).alias("avg_reaction_time_ms"),
        avg(col("movement_speed")).alias("avg_movement_speed"),
        avg(col("headshot_ratio")).alias("avg_headshot_ratio"),
        max(col("accuracy")).alias("max_accuracy"),
        min(col("reaction_time_ms")).alias("min_reaction_time_ms"),
        max(col("movement_speed")).alias("max_movement_speed"),
        avg(col("headshot_ratio")).alias("avg_headshot_ratio")
    ).withColumn(
        "kd_ratio",
        # K/D ratio with protection against division by zero
        col("total_kills") / (col("total_deaths") + 1)
    )
    
    # Read anomalies
    anomalies = spark.table(f"{ICEBERG_CATALOG}.{ANOMALY_TABLE}").filter(
        f"date(event_time) = date_sub(current_date(), 1)"
    )
    
    # Aggregate anomaly counts per player
    anomaly_counts = anomalies.groupBy(
        date_trunc("day", col("event_time")).alias("feature_date"),
        col("player_id")
    ).agg(
        count(col("event_id")).alias("total_anomalies"),
        count(distinct(col("anomaly_type"))).alias("distinct_anomaly_types")
    )
    
    # Join features with anomaly counts
    features_with_anomalies = player_features.join(
        anomaly_counts,
        on=["feature_date", "player_id"],
        how="left"
    ).fillna(0, subset=["total_anomalies"])
    
    # Calculate risk score (simple heuristic)
    # Higher score = more likely to be a cheater
    final_features = features_with_anomalies.withColumn(
        "risk_score",
        # Risk factors:
        # - High accuracy (>0.7): +0.2 per 0.1 above threshold
        # - Low reaction time (<150ms): +0.2 per 50ms below
        # - High movement speed (>10): +0.2 per 5 above
        # - Each anomaly: +0.1
        (
            (col("avg_accuracy") - 0.5) * 2 +
            (0.2 - col("avg_reaction_time_ms") / 1000) +
            (col("avg_movement_speed") - 5) / 25 +
            col("total_anomalies") * 0.1
        ).cast("double")
    ).withColumn(
        "last_updated",
        current_timestamp()
    ).select(
        col("feature_date").cast("date"),
        col("player_id"),
        col("total_sessions").cast("int"),
        col("total_matches").cast("int"),
        col("total_events").cast("int"),
        col("total_kills").cast("int"),
        col("total_deaths").cast("int"),
        col("total_assists").cast("int"),
        col("kd_ratio").cast("double"),
        col("avg_accuracy").cast("double"),
        col("avg_reaction_time_ms").cast("double"),
        col("avg_movement_speed").cast("double"),
        col("avg_headshot_ratio").cast("double"),
        col("max_accuracy").cast("double"),
        col("min_reaction_time_ms").cast("int"),
        col("max_movement_speed").cast("double"),
        col("total_anomalies").cast("int"),
        lit(None).alias("anomaly_types"),  # Simplified for MVP
        col("risk_score").cast("double"),
        col("last_updated").cast(TimestampType())
    )
    
    # Write to Iceberg table (overwrite partition)
    final_features.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("overwrite-mode", "dynamic") \
        .save(f"{ICEBERG_CATALOG}.{FEATURES_TABLE}")
    
    print(f"Features calculated and saved for {final_features.count()} players")


def run_batch_job(target_date=None):
    """Main batch job entry point."""
    print("Starting GameGuard daily features batch job...")
    
    spark = create_spark_session()
    
    # Ensure table exists
    ensure_features_table(spark)
    
    # Calculate features
    calculate_daily_features(spark, target_date)
    
    print("Batch job completed successfully!")
    
    spark.stop()


if __name__ == "__main__":
    # Accept optional date parameter
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_batch_job(target_date)
