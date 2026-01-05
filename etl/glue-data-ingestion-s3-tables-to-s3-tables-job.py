import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from awsglue.utils import getResolvedOptions

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DB',
    'SOURCE_TABLE',
    'TARGET_DB',
    'TARGET_TABLE',
    'UNIQUE_KEYS',  # Pass as comma-separated: "customer_id,order_date" or "NONE"
    'OPERATION_MODE'  # "merge" or "append"
])

# Parse unique keys
unique_keys_str = args.get('UNIQUE_KEYS', 'NONE')
UNIQUE_KEYS = None if unique_keys_str.upper() == 'NONE' else unique_keys_str.split(',')

SOURCE_DB = args['SOURCE_DB']
SOURCE_TABLE = args['SOURCE_TABLE']
TARGET_DB = args['TARGET_DB']
TARGET_TABLE = args['TARGET_TABLE']
OPERATION_MODE = args.get('OPERATION_MODE', 'merge' if UNIQUE_KEYS else 'append')

# ----------------------------------------------------
# Logger Setup
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------
# Configuration
# ----------------------------------------------------
catalog = "s3_tables"
logger.info("Starting Glue job for S3 Tables (Iceberg)")

try:
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("S3TablesGlueIntegration") \
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
        .getOrCreate()
    
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Using catalog: {catalog}")
    logger.info(f"Source table: {catalog}.{SOURCE_DB}.{SOURCE_TABLE}")
    logger.info(f"Target table: {catalog}.{TARGET_DB}.{TARGET_TABLE}")
    logger.info(f"Operation mode: {OPERATION_MODE}")
    logger.info(f"Unique keys: {UNIQUE_KEYS}")
    
    # Validate catalog
    logger.info("Validating catalog visibility")
    spark.sql('SHOW DATABASES').show(truncate=False)
    
    # Read source data
    logger.info("Reading source Iceberg table")
    source_df = spark.read.format("iceberg").load(f"{catalog}.{SOURCE_DB}.{SOURCE_TABLE}")
    
    # Convert any nanosecond timestamp columns to microseconds
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
    logger.info("Source schema:")
    source_df.printSchema()
    logger.info("Sample source data:")
    source_df.show(5, truncate=False)
    
    # Check if target table exists
    table_exists = spark.catalog.tableExists(f"{catalog}.{TARGET_DB}.{TARGET_TABLE}")
    logger.info(f"Target table exists: {table_exists}")
    
    if not table_exists:
        # Create empty table with schema first (for Athena compatibility)
        logger.info("Creating empty target table with schema")
        
        # Register source as temp view
        source_df.createOrReplaceTempView("source_temp")
        
        # Create table with WHERE 1=0 to create schema only
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{TARGET_DB}.{TARGET_TABLE}
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
        source_df.writeTo(f"{catalog}.{TARGET_DB}.{TARGET_TABLE}") \
            .using("iceberg") \
            .append()
        logger.info("Initial data inserted successfully")
        
    else:
        # Table exists - perform merge or append
        if OPERATION_MODE == "merge" and UNIQUE_KEYS:
            # MERGE operation (UPSERT)
            logger.info(f"Performing MERGE on keys: {UNIQUE_KEYS}")
            
            # Build merge condition
            merge_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in UNIQUE_KEYS
            ])
            
            # Get all columns for update
            all_columns = source_df.columns
            update_columns = ", ".join([f"{col} = source.{col}" for col in all_columns])
            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"source.{col}" for col in all_columns])
            
            # Register source as temp view
            source_df.createOrReplaceTempView("source_data")
            
            # Execute MERGE
            merge_query = f"""
            MERGE INTO {catalog}.{TARGET_DB}.{TARGET_TABLE} AS target
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
            source_df.writeTo(f"{catalog}.{TARGET_DB}.{TARGET_TABLE}") \
                .using("iceberg") \
                .append()
            logger.info("APPEND completed successfully")
    
    # Validate target table row count
    logger.info("Validating target table row count")
    target_count = spark.read.format("iceberg").load(f"{catalog}.{TARGET_DB}.{TARGET_TABLE}").count()
    logger.info(f"Target row count: {target_count}")
    
    # Show sample target data
    logger.info("Sample target data:")
    spark.read.format("iceberg").load(f"{catalog}.{TARGET_DB}.{TARGET_TABLE}").show(5, truncate=False)
    
    logger.info("SUCCESS: S3 Table operation completed")
    
except Exception as e:
    logger.error(f"ERROR: {str(e)}", exc_info=True)
    raise RuntimeError(e)
finally:
    spark.stop()
