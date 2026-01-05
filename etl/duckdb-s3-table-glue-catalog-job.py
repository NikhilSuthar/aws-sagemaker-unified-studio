import duckdb, boto3
conn = duckdb.connect("my_lakehouse.duckdb")
REGION = "ap-south-1"
ACCOUNT_ID = "442324907904"
glue_db = 'default'
glue_table = 'customer_stage'
TARGET_NAMESPACE = 'duckdb_poc_data'
TARGET_TABLE = "my_transformed_table_1"
session = boto3.Session()
credentials = session.get_credentials()
creds = credentials.get_frozen_credentials()
conn.execute("INSTALL aws;")
conn.execute("INSTALL httpfs;")
conn.execute("INSTALL iceberg;")
conn.execute("LOAD aws;")
conn.execute("LOAD httpfs;")
conn.execute("LOAD iceberg;")

def read_external_glue_table(database_name, table_name):
    """
    Read external (non-Iceberg) tables from Glue catalog
    """
    glue_client = boto3.client('glue', region_name=REGION)
    
    # Get table metadata
    response = glue_client.get_table(
        DatabaseName=database_name,
        Name=table_name
    )
    
    table_info = response['Table']
    s3_location = table_info['StorageDescriptor']['Location']
    table_type = table_info.get('Parameters', {}).get('table_type', 'EXTERNAL')
    
    print(f"Table: {database_name}.{table_name}")
    print(f"Type: {table_type}")
    print(f"Location: {s3_location}")
    
    # Determine file format
    input_format = table_info['StorageDescriptor'].get('InputFormat', '')
    
    if 'parquet' in input_format.lower():
        # Read Parquet
        query = f"SELECT * FROM read_parquet('{s3_location}*.parquet')"
    elif 'csv' in input_format.lower() or 'text' in input_format.lower():
        # Read CSV
        query = f"SELECT * FROM read_csv('{s3_location}*.csv', AUTO_DETECT=TRUE)"
    elif 'json' in input_format.lower():
        # Read JSON
        query = f"SELECT * FROM read_json('{s3_location}*.json', AUTO_DETECT=TRUE)"
    else:
        raise ValueError(f"Unsupported format: {input_format}")
    
    return conn.execute(query).fetchdf()


# Replace your secret creation with:
conn.execute("""
CREATE OR REPLACE SECRET s3_secret (
    TYPE S3,
    PROVIDER credential_chain,
    REGION 'ap-south-1'
);
""")

S3_TABLES_ARN = "arn:aws:s3tables:ap-south-1:442324907904:bucket/lumiq-pune-poc-iceberg-bucket"
conn.execute(f"""
 ATTACH '{S3_TABLES_ARN}' AS s3_tables (
     TYPE iceberg,
     ENDPOINT_TYPE s3_tables
 );
 """)
 
conn.execute(f"""
ATTACH '{ACCOUNT_ID}' AS glue_catalog (
    TYPE iceberg,
    ENDPOINT_TYPE glue
);
""")

print(f"Show databases")
conn.execute("SHOW DATABASES;").fetchdf()
print(f"Show tables")
conn.execute("SHOW ALL TABLES;").fetchdf()

source_data = read_external_glue_table(glue_db, glue_table)
print(f"✓ Read {len(source_data)} rows")
conn.register('source_df', source_data)
transformed = conn.execute("""
     SELECT
         *,
         CURRENT_TIMESTAMP as processed_at,
         'transformed_by_duckdb' as processing_status
     FROM source_df
 """).fetchdf()

conn.register('final_data', transformed)
conn.execute(f"""
     CREATE TABLE IF NOT EXISTS s3_tables.{TARGET_NAMESPACE}.{TARGET_TABLE} AS
     SELECT customer_id, customer_name, phone_number, email, pincode, account_balance FROM source_df
 """)
 