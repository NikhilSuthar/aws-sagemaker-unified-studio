import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

CATALOG = "s3tablescatalog"
catalog = CATALOG

SOURCE_S3_PATH = "s3://lumiq-pune-poc-bucket/data/cstr_stage/2.csv"
TARGET_DB = "duckdb_poc_data"
TARGET_TABLE = "customers_from_parquet"
UNIQUE_KEYS = ['customer_id'] 
OPERATION_MODE = "merge" if UNIQUE_KEYS else "append"

logger.info("Starting Glue job: S3 Parquet to S3 Tables")

try:
    # Initialize Spark with timestamp handling configurations
    spark = SparkSession.builder \
        .appName("S3ParquetToS3Tables") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", catalog) \
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog}.glue.id", "442324907904:s3tablescatalog/lumiq-pune-poc-iceberg-bucket") \
        .config(f"spark.sql.catalog.{catalog}.warehouse", "s3://sagemaker-unified-lakehouse/warehouse/") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "false") \
        .getOrCreate()
    
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Source S3 path: {SOURCE_S3_PATH}")
    logger.info(f"Target: {CATALOG}.{TARGET_DB}.{TARGET_TABLE}")
    
    # Read CSV/Parquet files from S3
    logger.info("Reading files from S3")
    source_df = spark.read.format('csv') \
        .option("mergeSchema", "true") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .option('inferschema', 'true') \
        .option('header', 'true') \
        .load(SOURCE_S3_PATH)
    
    # Convert timestamp columns to microsecond precision
    logger.info("Checking and converting timestamp columns")
    for field in source_df.schema.fields:
        if "timestamp" in str(field.dataType).lower():
            logger.info(f"Converting timestamp column: {field.name}")
            source_df = source_df.withColumn(
                field.name,
                col(field.name).cast(TimestampType())
            )
    
    source_count = source_df.count()
    logger.info(f"Source row count: {source_count}")
    
    # Show schema and sample
    logger.info("Source schema after conversion:")
    source_df.printSchema()
    logger.info("Sample data:")
    source_df.show(5, truncate=False)
    
    # Check if target table exists
    table_exists = spark.catalog.tableExists(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}")
    logger.info(f"Target table exists: {table_exists}")
    
    if not table_exists:
        # Create empty table with schema first (for Athena compatibility)
        logger.info("Creating empty target table with schema")
        
        # Register source as temp view
        source_df.createOrReplaceTempView("source_temp")
        
        # Create table with WHERE 1=0 to create schema only
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{TARGET_DB}.{TARGET_TABLE}
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '2',
                'write.parquet.compression-codec' = 'snappy'
            )
            AS
            SELECT * FROM source_temp WHERE 1 = 0
        """)
        
        logger.info("Empty target table created successfully")
        
        # Now insert the actual data
        logger.info("Inserting initial data into table")
        source_df.writeTo(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}") \
            .using("iceberg") \
            .append()
        logger.info("Initial data inserted")
        
    else:
        # Table exists - perform merge or append
        if OPERATION_MODE == "merge" and UNIQUE_KEYS:
            logger.info(f"Performing MERGE on keys: {UNIQUE_KEYS}")
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in UNIQUE_KEYS
            ])
            
            # Get all columns
            all_columns = source_df.columns
            update_columns = ", ".join([f"{col} = source.{col}" for col in all_columns])
            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"source.{col}" for col in all_columns])
            
            # Register source as temp view
            source_df.createOrReplaceTempView("source_data")
            
            # Execute MERGE
            merge_query = f"""
            MERGE INTO {CATALOG}.{TARGET_DB}.{TARGET_TABLE} AS target
            USING source_data AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
            
            logger.info("Executing MERGE query")
            spark.sql(merge_query)
            logger.info("MERGE completed successfully")
            
        else:
            # APPEND operation
            logger.info("Performing APPEND operation")
            source_df.writeTo(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}") \
                .using("iceberg") \
                .append()
            logger.info("APPEND completed successfully")
    
    # Validate target table
    logger.info("Validating target table row count")
    target_count = spark.read.format("iceberg").load(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}").count()
    logger.info(f"Target row count: {target_count}")
    
    # Show sample target data
    logger.info("Sample target data:")
    spark.read.format("iceberg").load(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}").show(5, truncate=False)
    
    logger.info("SUCCESS: S3 CSV/Parquet to S3 Table completed")
    
except Exception as e:
    logger.error(f"ERROR: {str(e)}", exc_info=True)
    raise RuntimeError(e)
finally:
    spark.stop()
