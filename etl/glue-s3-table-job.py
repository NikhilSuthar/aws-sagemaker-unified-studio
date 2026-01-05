import sys
import logging
from pyspark.sql import SparkSession

# ----------------------------------------------------
# Logger Setup
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)
CATALOG = "s3tablescatalog" 
catalog =  CATALOG
logger.info("Starting Glue job for S3 Tables (Iceberg)")
try:
    # spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder \
        .appName("S3TablesGlueIntegration") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog",catalog) \
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog}.glue.id", "442324907904:s3tablescatalog/lumiq-pune-poc-iceberg-bucket") \
        .config(f"spark.sql.catalog.{catalog}.warehouse", "s3://sagemaker-unified-lakehouse/warehouse/") \
        .getOrCreate()
    logger.info(f"Spark version: {spark.version}")
    SOURCE_DB = "duckdb_poc_data"
    SOURCE_TABLE = "cstr_dtls"
    TARGET_DB = "duckdb_poc_data"
    TARGET_TABLE = "my_iceberg_table54"

    logger.info(f"Using catalog: {CATALOG}")
    logger.info(f"Source table: {CATALOG}.{SOURCE_DB}.{SOURCE_TABLE}")
    logger.info(f"Target table: {CATALOG}.{TARGET_DB}.{TARGET_TABLE}")
    logger.info("Validating catalog visibility")
    spark.sql('SHOW DATABASES').show(truncate=False)
    spark.sql(f"SHOW DATABASES IN {CATALOG}").show(truncate=False)

# try:
    logger.info("Creating target Iceberg table if not exists")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{TARGET_DB}.{TARGET_TABLE}
        USING iceberg
        AS
        SELECT *
        FROM {CATALOG}.{SOURCE_DB}.{SOURCE_TABLE}
        WHERE 1 = 0
    """)

    logger.info("Target table created or already exists")

# except Exception as e:
#     fail("Target table creation", e)
    logger.info("Reading source Iceberg table")
    df = spark.read.format("iceberg").load(f"{CATALOG}.{SOURCE_DB}.{SOURCE_TABLE}")
    source_count = df.count()
    logger.info(f"Source row count: {source_count}")
    logger.info("Writing data to target Iceberg table")
    # df.write.format("iceberg").mode("overwrite").save(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}")
    df.writeTo(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}").using("Iceberg").tableProperty ("format-version", "2").createOrReplace()
    logger.info("Write completed successfully")
    logger.info("Validating target table row count")
    target_count = spark.read.format("iceberg").load(f"{CATALOG}.{TARGET_DB}.{TARGET_TABLE}").count()
    logger.info(f"Target row count: {target_count}")
    logger.info("SUCCESS: S3 Table Iceberg copy completed")
except Exception as e:
    logger.error(e)
    raise RuntimeError(e)