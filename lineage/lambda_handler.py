# lambda_function.py - OpenLineage + Generic handler
import json
import os
from lineage_poster import DataZoneLineagePoster

# Configuration
DOMAIN_ID = os.environ.get('DOMAIN_ID', 'dzd_3oxfuz0uw1456p')
PROJECT_ID = os.environ.get('PROJECT_ID', '5w2evsosqwud8h')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-south-1')
DATA_SOURCE_TYPE = os.environ.get('DATA_SOURCE_TYPE', 'iceberg')
JOB_INTEGRATION = os.environ.get('JOB_INTEGRATION', 'DBT')

# S3 Tables configuration
S3_TABLES_ACCOUNT = os.environ.get('S3_TABLES_ACCOUNT', '442324907904')
S3_TABLES_CATALOG = os.environ.get('S3_TABLES_CATALOG', 's3tablescatalog')
S3_TABLES_DATABASE = os.environ.get('S3_TABLES_DATABASE', 'lumiq-pune-poc-iceberg-bucket')
DEFAULT_SCHEMA = os.environ.get('DEFAULT_SCHEMA', 'duckdb_poc_data')

# Initialize poster
poster = DataZoneLineagePoster(
    domain_id=DOMAIN_ID,
    project_id=PROJECT_ID,
    region=AWS_REGION,
    data_source_type=DATA_SOURCE_TYPE,
    job_integration=JOB_INTEGRATION
)


def lambda_handler(event, context):
    """
    Unified handler for:
    1. OpenLineage events from dbt
    2. Generic lineage API calls
    """
    
    print("="*60)
    print("DataZone Lineage Handler")
    print("="*60)
    
    try:
        # Parse event
        body = parse_event(event)
        
        # Detect event type
        if is_openlineage_event(body):
            print("Event type: OpenLineage")
            return handle_openlineage_event(body)
        else:
            print("Event type: Generic API")
            return handle_generic_event(body)
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
        }


def parse_event(event):
    """
    Parse event from different sources
    """
    
    # Lambda Function URL / API Gateway
    if isinstance(event, dict) and 'body' in event:
        body = event['body']
        if isinstance(body, str):
            return json.loads(body)
        return body
    
    # Direct invocation
    if isinstance(event, str):
        return json.loads(event)
    
    return event


def is_openlineage_event(body):
    """
    Detect if event is OpenLineage format
    """
    return (
        isinstance(body, dict) and
        'eventType' in body and
        'run' in body and
        'job' in body
    )


def handle_openlineage_event(ol_event):
    """
    Handle OpenLineage event from dbt
    
    OpenLineage event structure:
    {
      "eventType": "START|COMPLETE|FAIL",
      "eventTime": "...",
      "run": {"runId": "..."},
      "job": {"namespace": "dbt", "name": "model_name"},
      "inputs": [{"namespace": "...", "name": "..."}],
      "outputs": [{"namespace": "...", "name": "..."}],
      "producer": "dbt/..."
    }
    """
    
    print("\n" + "-"*60)
    print("Processing OpenLineage Event")
    print("-"*60)
    
    event_type = ol_event.get('eventType')
    run_id = ol_event.get('run', {}).get('runId')
    job_name = ol_event.get('job', {}).get('name')
    
    print(f"  Event Type: {event_type}")
    print(f"  Job: {job_name}")
    print(f"  Run ID: {run_id}")
    
    # Only process COMPLETE events (or you can process START too)
    if event_type not in ['COMPLETE', 'START']:
        print(f"  Skipping {event_type} event")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Event type {event_type} acknowledged but not processed',
                'runId': run_id,
                'jobName': job_name
            })
        }
    
    # Extract inputs and outputs
    inputs = ol_event.get('inputs', [])
    outputs = ol_event.get('outputs', [])
    
    if not inputs or not outputs:
        print("  Warning: No inputs or outputs found")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event acknowledged but no lineage to post (no inputs/outputs)',
                'runId': run_id,
                'jobName': job_name
            })
        }
    
    # Extract SQL query if present
    sql_query = extract_sql_query(ol_event)
    
    # Extract schema if present
    schema_fields = extract_schema(outputs[0] if outputs else None)
    
    # Convert OpenLineage datasets to our format
    source_tables = []
    for inp in inputs:
        table_info = parse_openlineage_dataset(inp)
        if table_info:
            source_tables.append(table_info)
    
    target_table = None
    if outputs:
        target_table = parse_openlineage_dataset(outputs[0])
    
    if not source_tables or not target_table:
        print("  Warning: Could not parse source/target tables")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event acknowledged but could not parse table references',
                'runId': run_id,
                'jobName': job_name
            })
        }
    
    print(f"\n  Source tables: {[f'{t["schema"]}.{t["table"]}' for t in source_tables]}")
    print(f"  Target table: {target_table['schema']}.{target_table['table']}")
    if sql_query:
        print(f"  SQL Query: {sql_query[:100]}...")
    
    # Post lineage to DataZone
    result = poster.post_table_lineage(
        source_tables=source_tables,
        target_table=target_table,
        job_name=job_name,
        sql_query=sql_query,
        run_id=run_id,
        metadata={'schema': schema_fields} if schema_fields else None
    )
    
    print("\n✓ OpenLineage event processed successfully")
    
    return {
        'statusCode': 200,
        'body': json.dumps(result, default=str)
    }


def handle_generic_event(body):
    """
    Handle generic API event (non-OpenLineage)
    """
    
    print("\n" + "-"*60)
    print("Processing Generic API Event")
    print("-"*60)
    
    source_tables = body.get('source_tables', [])
    target_table = body.get('target_table')
    job_name = body.get('job_name')
    sql_query = body.get('sql_query')
    metadata = body.get('metadata', {})
    run_id = body.get('run_id')
    job_type_override = body.get('job_type_override')
    
    if not source_tables or not target_table or not job_name:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Missing required fields: source_tables, target_table, job_name'
            })
        }
    
    result = poster.post_table_lineage(
        source_tables=source_tables,
        target_table=target_table,
        job_name=job_name,
        sql_query=sql_query,
        run_id=run_id,
        metadata=metadata,
        job_type_override=job_type_override
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(result, default=str)
    }


def parse_openlineage_dataset(dataset):
    """
    Parse OpenLineage dataset to our table format
    
    OpenLineage dataset namespace examples:
    - "dbt://..."
    - "s3://..."
    - Just table name
    
    We need to extract: account_id, catalog, database, schema, table
    """
    
    if not dataset:
        return None
    
    dataset_name = dataset.get('name', '')
    dataset_namespace = dataset.get('namespace', '')
    
    print(f"\n  Parsing dataset:")
    print(f"    Namespace: {dataset_namespace}")
    print(f"    Name: {dataset_name}")
    
    # Try to extract table name from dataset name
    # Common patterns:
    # - "schema.table"
    # - "database.schema.table"
    # - Just "table"
    
    parts = dataset_name.split('.')
    
    if len(parts) >= 2:
        schema = parts[-2]
        table = parts[-1]
    elif len(parts) == 1:
        schema = DEFAULT_SCHEMA
        table = parts[0]
    else:
        print(f"    Warning: Could not parse dataset name: {dataset_name}")
        return None
    
    # Construct table info
    table_info = {
        'account_id': S3_TABLES_ACCOUNT,
        'catalog': S3_TABLES_CATALOG,
        'database': S3_TABLES_DATABASE,
        'schema': schema,
        'table': table
    }
    
    print(f"    Parsed as: {schema}.{table}")
    
    return table_info


def extract_sql_query(ol_event):
    """
    Extract SQL query from OpenLineage event
    """
    
    # Check job facets for SQL
    job = ol_event.get('job', {})
    job_facets = job.get('facets', {})
    
    if 'sql' in job_facets:
        return job_facets['sql'].get('query')
    
    # Check for dbt-specific SQL facet
    if 'dbt' in job_facets:
        return job_facets['dbt'].get('compiled_sql') or job_facets['dbt'].get('raw_sql')
    
    return None


def extract_schema(output_dataset):
    """
    Extract schema from output dataset
    """
    
    if not output_dataset:
        return None
    
    facets = output_dataset.get('facets', {})
    schema_facet = facets.get('schema')
    
    if schema_facet and 'fields' in schema_facet:
        return schema_facet['fields']
    
    return None
