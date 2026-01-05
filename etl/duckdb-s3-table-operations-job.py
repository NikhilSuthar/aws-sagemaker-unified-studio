import duckdb, boto3
conn = duckdb.connect("my_lakehouse1.duckdb")
REGION = "ap-south-1"
ACCOUNT_ID = "442324907904"
session = boto3.Session()
credentials = session.get_credentials()
creds = credentials.get_frozen_credentials()
conn.execute("INSTALL aws;")
conn.execute("INSTALL httpfs;")
conn.execute("INSTALL iceberg;")
conn.execute("LOAD aws;")
conn.execute("LOAD httpfs;")
conn.execute("LOAD iceberg;")
 
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


conn.execute(f"""
ATTACH '{ACCOUNT_ID}' AS glue_catalog (
    TYPE iceberg,
    ENDPOINT_TYPE glue
);
""")
print(f"Show databases")
df = conn.execute("SHOW DATABASES;").fetchdf()
df
print(f"Show tables")
df1 = conn.execute("SHOW ALL TABLES;").fetchdf()
df1
NAMESPACE = "duckdb_poc_data"
TABLE_NAME = "stg_cust_3"
df = conn.execute(f"""
     SELECT *
     FROM s3_tables.{NAMESPACE}.{TABLE_NAME}
     LIMIT 10
 """).fetchdf()
df

glue_db = 'default'
glue_table = 'cust_dtls'
df1 = conn.execute(f"""
     SELECT *
     FROM glue_catalog.{glue_db}.{glue_table}
     LIMIT 10
 """).fetchdf()
df1




   customer_id first_name last_name                    email signup_date
0          101      Alice     Smith  alice.smith@example.com  2023-01-15
1          102        Bob   Johnson        bob.j@example.com  2023-03-10
2          103    Charlie  Williams    charlie.w@example.com  2023-05-22
3          104      David     Brown      david.b@example.com  2023-07-01
4          105        Eva     Jones    eva.jones@example.com  2023-09-19
count = conn.execute(f"""
     SELECT COUNT(*) as total
     FROM s3_tables.{NAMESPACE}.{TABLE_NAME}
 """).fetchone()
print(f"\nTotal rows in table: {count[0]}")

Total rows in table: 5
SOURCE_NAMESPACE = "duckdb_poc_data"
SOURCE_TABLE = "my_iceberg_table3"
TARGET_TABLE = "my_transformed_table"
source_data = conn.execute(f"""
     SELECT *
     FROM s3_tables.{SOURCE_NAMESPACE}.{SOURCE_TABLE}
 """).fetchdf()
print(f"✓ Read {len(source_data)} rows")
✓ Read 5 rows
conn.register('source_df', source_data)
transformed = conn.execute("""
     SELECT
         *,
         CURRENT_TIMESTAMP as processed_at,
         'transformed_by_duckdb' as processing_status
     FROM source_df
 """).fetchdf()

print(f"✓ Transformed {len(transformed)} rows")
✓ Transformed 5 rows
conn.register('final_data', transformed)
<_duckdb.DuckDBPyConnection object at 0x7611f9e918f0>
conn.execute(f"""
     CREATE TABLE IF NOT EXISTS s3_tables.{SOURCE_NAMESPACE}.{TARGET_TABLE} AS
     SELECT * FROM final_data
 """)
<_duckdb.DuckDBPyConnection object at 0x7611f9e918f0>
verify_count = conn.execute(f"""
     SELECT COUNT(*)
     FROM s3_tables.{SOURCE_NAMESPACE}.{TARGET_TABLE}
 """).fetchone()
print(f"✓ Verified {verify_count[0]} rows in new table")
✓ Verified 5 rows in new table


print("\n=== INSERT Data ===")
# Insert data
conn.execute(f"""
INSERT INTO s3_tables.{NAMESPACE}.{TARGET_TABLE}
VALUES 
    (1, 'A', 'B', 'A.B@email.com', '2023-01-23', CURRENT_TIMESTAMP,'Start'),
    (2, 'C', 'D', 'C.D@email.com', '2023-06-29', CURRENT_TIMESTAMP,'Complete')
""")
print("✓ Inserted 3 rows")


conn.execute(f"""
CREATE OR REPLACE TEMP VIEW temp_vw AS
SELECT *
FROM (
    VALUES
        (1, 'Nikhil', 'Nikhil@email.com', 23453453453245,'' ,11, '', CURRENT_TIMESTAMP, '',''),
        (9666, 'Customer_9', 'user91@email.com', 9372305070,'Delhi' ,55, 's3://lumiq-pune-poc-bucket/duckdb_POC/customer_2.csv', CURRENT_TIMESTAMP, 'India','')
) AS t (
customerid,
customername,
email,
phone,
city,
age,
source_file,
insertedtimestamp,
country,
file_name
)
""")
temp_df = conn.execute(f""" SELECT * FROM temp_vw """).fetchdf()


conn.execute(f"""
MERGE INTO s3_tables.{NAMESPACE}.{TARGET_TABLE} AS tgt
USING temp_vw AS src
ON tgt.customerid = src.customerid
WHEN MATCHED THEN
  UPDATE SET 
    customername = src.customername,
    phone  = src.phone,
    email      = src.email,
    city = src.city,
    age = src.age,
    source_file     = src.source_file,
    insertedtimestamp     = src.insertedtimestamp,
    country     = src.country,
    file_name     = src.file_name
WHEN NOT MATCHED THEN
    INSERT (
    customerid,
    customername,
    email,
    phone,
    city,
    age,
    source_file,
    insertedtimestamp,
    country,
    file_name
  )
  VALUES (
    src.customerid,
    src.customername,
    src.email,
    src.phone,
    src.city,
    src.age,
    src.source_file,
    src.insertedtimestamp,
    src.country,
    src.file_name
  );
""")


 # close any open transaction

conn.execute(f"""
BEGIN;

CREATE OR REPLACE TEMP TABLE staged AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    processed_at,
    processing_status
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY processed_at DESC
        ) AS rn
    FROM temp_vw
)
WHERE rn = 1;

DELETE FROM s3_tables.{NAMESPACE}.my_transformed_table
WHERE customer_id IN (
    SELECT customer_id FROM staged
);

INSERT INTO s3_tables.{NAMESPACE}.my_transformed_table
SELECT * FROM staged;

COMMIT;
""")


conn.execute(f"""ALTER TABLE s3_tables.{NAMESPACE}.my_transformed_table
ADD COLUMN source_system VARCHAR;
""")



conn.execute(f"""CREATE OR REPLACE TABLE s3_tables.{NAMESPACE}.duckdb_table AS SELECT * FROM temp_vw""")




conn.execute(f"""
BEGIN;

DELETE FROM s3_tables.{NAMESPACE}.my_transformed_table
WHERE customer_id IN (SELECT customer_id FROM temp_vw);

INSERT INTO s3_tables.{NAMESPACE}.my_transformed_table
SELECT customer_id, first_name, last_name
FROM temp_vw;

COMMIT;
""")

conn.execute(f"""
UPDATE s3_tables.{NAMESPACE}.{TARGET_TABLE}
SET city = 'AMD',
insertedtimestamp = CURRENT_TIMESTAMP,
age = 99
WHERE customerid in (9799,9755) 100
""")


conn.execute(f"""
UPDATE s3_tables.{NAMESPACE}.{TARGET_TABLE}
SET processed_at = CURRENT_TIMESTAMP,
last_name = 'dummy'
WHERE customer_id < 100
""")

conn.execute(f"""
DELETE FROM s3_tables.{NAMESPACE}.{TARGET_TABLE}
WHERE customer_id = 2
""")


def upsert_data(source_data, target_table, unique_keys):
    """
    UPSERT using DELETE + INSERT in a transaction
    
    Args:
        source_data: DataFrame or table name with new data
        target_table: Target S3 table name (e.g., 'namespace.table')
        unique_keys: List of columns that define uniqueness (e.g., ['id', 'date'])
    """
    conn = duckdb.connect("my_lakehouse.duckdb")
    conn.execute("LOAD aws; LOAD httpfs; LOAD iceberg;")
    conn.execute("CREATE SECRET IF NOT EXISTS (TYPE s3, PROVIDER credential_chain);")
    conn.execute(f"""
    ATTACH '{S3_TABLES_ARN}' AS s3_tables (
        TYPE iceberg,
        ENDPOINT_TYPE s3_tables
    );
    """)
    
    try:
        print("=== Starting UPSERT Transaction ===")
        
        # BEGIN TRANSACTION
        conn.execute("BEGIN TRANSACTION;")
        
        # Register source data
        conn.register('source_data', source_data)
        
        # Step 1: DELETE existing records based on unique keys
        print("\n1. Deleting existing records...")
        where_conditions = " OR ".join([
            f"({' AND '.join([f'{target_table}.{key} = source_data.{key}' for key in unique_keys])})"
        ])
        
        # Build delete condition
        delete_keys = ', '.join([f'source_data.{key}' for key in unique_keys])
        key_tuple = f"({', '.join(unique_keys)})"
        
        delete_sql = f"""
        DELETE FROM {target_table}
        WHERE {key_tuple} IN (
            SELECT {delete_keys} FROM source_data
        )
        """
        
        result = conn.execute(delete_sql)
        deleted_count = result.fetchone()[0] if result else 0
        print(f"✓ Deleted {deleted_count} existing records")
        
        # Step 2: INSERT new records
        print("\n2. Inserting new records...")
        insert_sql = f"""
        INSERT INTO {target_table}
        SELECT * FROM source_data
        """
        
        conn.execute(insert_sql)
        inserted_count = len(source_data)
        print(f"✓ Inserted {inserted_count} new records")
        
        # COMMIT TRANSACTION
        conn.execute("COMMIT;")
        print("\n✓ Transaction committed successfully")
        
        return {"deleted": deleted_count, "inserted": inserted_count}
        
    except Exception as e:
        # ROLLBACK on error
        print(f"\n✗ Error occurred: {e}")
        print("Rolling back transaction...")
        conn.execute("ROLLBACK;")
        print("✓ Transaction rolled back")
        raise
        
    finally:
        conn.close()
        
        


conn.execute("""
CREATE TABLE IF NOT EXISTS dummy_src_table (
    customer_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    signup_date TIMESTAMP,
    processed_at TIMESTAMP,
    processing_status VARCHAR
);
""")


conn.execute("""
INSERT INTO dummy_src_table VALUES
    (1, 'A', 'B', 'a.dummy@email.com', '2023-01-29', CURRENT_TIMESTAMP, 'Complete'),
    (103, 'Charlie', 'Williams', 'charlie.Williams@example.com', '2023-05-22', CURRENT_TIMESTAMP, 'Complete'),
""")


def incremental_upsert(conn, source_table, target_table, unique_keys, incremental_column='updated_at'):
    """
    UPSERT from local DuckDB table to S3 Table
    
    Args:
        source_table: Local table name (e.g., 'source_table')
        target_table: S3 table name (e.g., 's3_tables.duckdb_poc_data.customer_data')
        unique_keys: List of columns for uniqueness (e.g., ['id'])
        incremental_column: Column for incremental logic (e.g., 'updated_at')
    """
    
    # Attach S3 Table    
    transaction_started = False
    
    try:
        print("=== Incremental UPSERT (Local -> S3) ===")
        
        # Step 1: Get last update timestamp from target
        print("\n1. Getting last update timestamp...")
        
        # Use proper SQL string (not f-string in execute)
        last_update_sql = f"SELECT MAX({incremental_column}) as max_date FROM {target_table}"
        
        try:
            result = conn.execute(last_update_sql).fetchone()
            last_update = result[0] if result else None
        except Exception as e:
            # Table might not exist yet
            print(f"  Note: {e}")
            last_update = None
        
        if last_update is None:
            print("  No existing data in target, loading all records")
            filter_clause = ""
        else:
            print(f"  Last update in target: {last_update}")
            filter_clause = f"WHERE {incremental_column} > TIMESTAMP '{last_update}'"
        
        # Step 2: Get incremental data from local source
        print("\n2. Extracting incremental data from local source...")
        
        # Build SQL string properly
        select_sql = f"SELECT * FROM {source_table} {filter_clause}"
        incremental_data = conn.execute(select_sql).fetchdf()
        
        print(f"✓ Found {len(incremental_data)} new/updated records")
        
        if len(incremental_data) == 0:
            print("  No new data to process")
            conn.close()
            return {"deleted": 0, "inserted": 0}
        
        print("\nIncremental data:")
        print(incremental_data)
        
        # Step 3: UPSERT with DELETE + INSERT in transaction
        print("\n3. Performing UPSERT to S3 Table...")
        
        # BEGIN TRANSACTION
        conn.execute("BEGIN TRANSACTION;")
        transaction_started = True
        
        # Register incremental data as a temporary table
        conn.register('inc_data', incremental_data)
        
        # Build DELETE SQL
        if len(unique_keys) == 1:
            # Single key
            delete_sql = f"""
            DELETE FROM {target_table}
            WHERE {unique_keys[0]} IN (
                SELECT {unique_keys[0]} FROM inc_data
            )
            """
        else:
            # Multiple keys (composite)
            key_tuple = f"({', '.join(unique_keys)})"
            select_keys = ', '.join(unique_keys)
            delete_sql = f"""
            DELETE FROM {target_table}
            WHERE {key_tuple} IN (
                SELECT {select_keys} FROM inc_data
            )
            """
        
        # Execute DELETE
        print(f"  Executing DELETE...")
        deleted_result = conn.execute(delete_sql).fetchone()
        deleted_count = deleted_result[0] if deleted_result else 0
        print(f"  ✓ Deleted {deleted_count} existing records")
        
        # Execute INSERT
        print(f"  Executing INSERT...")
        insert_sql = f"INSERT INTO {target_table} SELECT * FROM inc_data"
        conn.execute(insert_sql)
        inserted_count = len(incremental_data)
        print(f"  ✓ Inserted {inserted_count} new records")
        
        # COMMIT
        conn.execute("COMMIT;")
        transaction_started = False
        print("\n✓ Transaction committed successfully")
        
        # Step 4: Verify final state
        print("\n4. Verifying target table...")
        final_count_sql = f"SELECT COUNT(*) FROM {target_table}"
        final_count = conn.execute(final_count_sql).fetchone()[0]
        print(f"✓ Target table now has {final_count} total rows")
        
        # Show sample of target
        sample_sql = f"SELECT * FROM {target_table} ORDER BY id LIMIT 10"
        sample = conn.execute(sample_sql).fetchdf()
        print("\nTarget table sample:")
        print(sample)
        
        return {
            "deleted": deleted_count,
            "inserted": inserted_count,
            "total_in_target": final_count
        }
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        
        # Only rollback if transaction was started
        if transaction_started:
            print("Rolling back transaction...")
            try:
                conn.execute("ROLLBACK;")
                print("✓ Transaction rolled back")
            except:
                pass  # Transaction might have already been rolled back
        
        raise
        
    finally:
        conn.close()


result = incremental_upsert(source_table='dummy_src_table', target_table='s3_tables.duckdb_poc_data.target_table',unique_keys=['customer_id'],incremental_column='processed_at')







# ~/.dbt/profiles.yml
my_athena_project:
  target: dev
  outputs:
    dev:
      type: athena
      
      # S3 Configuration
      s3_staging_dir: s3://lumiq-pune-poc-bucket /dbt-staging/
      s3_data_dir: s3://lumiq-pune-poc-bucket/dbt-output/
      s3_data_naming: schema_table
      
      # AWS Configuration - Use IAM instance role
      region_name: ap-south-1
      # Remove or comment out aws_profile_name for EC2
      # aws_profile_name: default
      
      # S3 Tables Catalog Configuration
      database: s3tablescatalog/lumiq-pune-poc-iceberg-bucket
      schema: duckdb_poc_data
      work_group: primary
      
      # Performance
      threads: 4
      num_retries: 3
      poll_interval: 5
      
      # Iceberg Configuration
      table_type: iceberg





aws lakeformation grant-permissions \
--region ap-south-1 \
--cli-input-json \
'{
    "Principal": {
        "DataLakePrincipalIdentifier": "arn:aws:iam::442324907904:role/forensic-insight-ec2-role"
    },
    "Resource": {
        "Catalog": {
            "Id":"442324907904:s3tablescatalog/lumiq-pune-poc-iceberg-bucket"
        }
    },
    "Permissions": ["ALL"]
}'