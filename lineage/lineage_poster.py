# lineage_poster.py - Updated with better icon mappings
import json
import boto3
from datetime import datetime
from uuid import uuid4
from typing import Dict, List, Optional

class DataZoneLineagePoster:
    """
    Generic DataZone lineage poster with configurable icons
    """
    
    def __init__(
        self, 
        domain_id: str, 
        project_id: str, 
        region: str = 'ap-south-1',
        data_source_type: str = 'ICEBERG',  # Controls dataset icon
        job_integration: str = 'SPARK'      # Controls job icon
    ):
        self.domain_id = domain_id
        self.project_id = project_id
        self.region = region
        self.data_source_type = data_source_type
        self.job_integration = job_integration
        self.client = boto3.client('datazone', region_name=region)
        
        # Base namespace for Glue S3 Tables
        self.glue_namespace = f"arn:aws:glue:{region}"
    
    def construct_source_identifier(
        self,
        account_id: str,
        catalog: str,
        database: str,
        schema: str,
        table: str
    ) -> str:
        """
        Construct exact sourceIdentifier for S3 Tables
        """
        return f"{self.glue_namespace}:{account_id}:table/{catalog}/{database}/{schema}/{table}"
    
    def post_table_lineage(
        self,
        source_tables: List[Dict[str, str]],
        target_table: Dict[str, str],
        job_name: str,
        sql_query: Optional[str] = None,
        run_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        job_type_override: Optional[str] = None
    ) -> Dict:
        """
        Post lineage for a transformation
        
        Args:
            source_tables: List of source table dicts
            target_table: Target table dict
            job_name: Name of the job/model
            sql_query: SQL query text
            run_id: Optional run ID
            metadata: Additional metadata (schema, etc.)
            job_type_override: Override job integration (e.g., 'SPARK', 'DBT', 'PYTHON')
        """
        
        if not run_id:
            run_id = str(uuid4())
        
        job_integration = job_type_override or self.job_integration
        
        print(f"\nPosting lineage for job: {job_name}")
        print(f"  Sources: {[f'{s["schema"]}.{s["table"]}' for s in source_tables]}")
        print(f"  Target: {target_table['schema']}.{target_table['table']}")
        print(f"  Job Type: {job_integration}")
        print(f"  Data Source: {self.data_source_type}")
        print(f"  Run ID: {run_id}")
        
        # Post START event
        start_response = self._post_event(
            event_type='START',
            run_id=run_id,
            source_tables=source_tables,
            target_table=target_table,
            job_name=job_name,
            sql_query=sql_query,
            metadata=metadata,
            job_integration=job_integration
        )
        
        # Post COMPLETE event
        complete_response = self._post_event(
            event_type='COMPLETE',
            run_id=run_id,
            source_tables=source_tables,
            target_table=target_table,
            job_name=job_name,
            sql_query=sql_query,
            metadata=metadata,
            job_integration=job_integration
        )
        
        return {
            'success': True,
            'runId': run_id,
            'jobName': job_name,
            'jobIntegration': job_integration,
            'dataSourceType': self.data_source_type,
            'startEventId': start_response.get('id'),
            'completeEventId': complete_response.get('id'),
            'sourceCount': len(source_tables),
            'target': f"{target_table['schema']}.{target_table['table']}"
        }
    
    def _post_event(
        self,
        event_type: str,
        run_id: str,
        source_tables: List[Dict],
        target_table: Dict,
        job_name: str,
        sql_query: Optional[str],
        metadata: Optional[Dict],
        job_integration: str
    ) -> Dict:
        """
        Post a single lineage event
        """
        
        event_time = datetime.utcnow().isoformat() + 'Z'
        
        # Namespace for sourceIdentifier matching
        namespace = f"arn:aws:glue:{self.region}:{source_tables[0]['account_id']}:table"
        
        # Build inputs
        inputs = []
        for src in source_tables:
            name = f"{src['catalog']}/{src['database']}/{src['schema']}/{src['table']}"
            
            inputs.append({
                "namespace": namespace,
                "name": name,
                "facets": {
                    "dataSource": {
                        "_producer": f"dbt-{self.project_id}",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json",
                        "name": self.data_source_type.lower(),
                        "uri": self.construct_source_identifier(
                            src['account_id'], src['catalog'], src['database'],
                            src['schema'], src['table']
                        )
                    }
                }
            })
        
        # Build output
        target_name = f"{target_table['catalog']}/{target_table['database']}/{target_table['schema']}/{target_table['table']}"
        
        output_facets = {
            "dataSource": {
                "_producer": f"dbt-{self.project_id}",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json",
                "name": self.data_source_type.lower(),
                "uri": self.construct_source_identifier(
                    target_table['account_id'], target_table['catalog'],
                    target_table['database'], target_table['schema'], target_table['table']
                )
            }
        }
        
        # Add schema if provided
        if metadata and 'schema' in metadata:
            output_facets['schema'] = {
                "_producer": f"dbt-{self.project_id}",
                "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
                "fields": metadata['schema']
            }
        
        outputs = [{
            "namespace": namespace,
            "name": target_name,
            "facets": output_facets
        }]
        
        # Build job facets
        job_facets = {
            "jobType": {
                "_producer": f"dbt-{self.project_id}",
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json",
                "integration": job_integration,
                "jobType": "JOB",
                "processingType": "BATCH"
            }
        }
        
        # Add SQL query facet
        if sql_query:
            job_facets['sql'] = {
                "_producer": f"dbt-{self.project_id}",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SQLJobFacet.json",
                "query": sql_query
            }
        
        # Build OpenLineage event
        lineage_event = {
            "eventType": event_type,
            "eventTime": event_time,
            "producer": f"dbt-{self.project_id}",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {
                "runId": run_id,
                "facets": {}
            },
            "job": {
                "namespace": f"dbt-{self.project_id}",
                "name": job_name,
                "facets": job_facets
            },
            "inputs": inputs,
            "outputs": outputs
        }
        
        # Post to DataZone
        response = self.client.post_lineage_event(
            domainIdentifier=self.domain_id,
            event=json.dumps(lineage_event)
        )
        
        print(f"  ✓ {event_type} event posted: {response.get('id')}")
        
        return response

