import boto3
from datetime import datetime, timezone
import json

# Configuration
DOMAIN_ID = 'dzd_3oxfuz0uw1456p'
PROJECT_ID = '5w2evsosqwud8h'
asset_name = 'orders_iceberg'
REGION = 'ap-south-1'

client = boto3.client('datazone', region_name=REGION)
def get_asset_id(asset_name: str, domain_id: str, project_id: str = None, region: str = 'ap-south-1') -> str:
    """
    Get asset ID by searching for asset name
    
    Args:
        asset_name: Name of the asset (e.g., 'stg_cust_new2')
        domain_id: DataZone domain ID
        project_id: Optional project ID to filter results
        region: AWS region
    
    Returns:
        Asset ID as string, or None if not found
    """
    
    client = boto3.client('datazone', region_name=region)
    
    # Search for asset
    response = client.search(
        domainIdentifier=domain_id,
        searchScope='ASSET',
        searchText=asset_name,
        owningProjectIdentifier=project_id,
        additionalAttributes=['FORMS']  # Get forms including DQ data
    )
    
    # Find exact match
    for item in response.get('items', []):
        if 'assetItem' in item:
            asset = item['assetItem']
            if asset.get('name', '').lower() == asset_name.lower():
                print(f"✅ Found: {asset['name']} -> {asset['identifier']}")
                return asset['identifier']
    
    print(f"❌ Asset not found: {asset_name}")
    return None


# Your DQ data
dq_data = {
    "evaluations": [
        {"types": ["Completeness"], "description": "customer_id not null", "status": "PASS", "applicableFields": ["customer_id"]},
        {"types": ["Validity"], "description": "status check", "status": "FAIL", "applicableFields": ["status"]},
            {"types": ["Validity"], "description": "age column not found check", "status": "FAIL", "applicableFields": ["status"]},
        {"types": ["Validity"], "description": "time stamp error check", "status": "FAIL", "applicableFields": ["status"]}
    ]
}

count = len(dq_data['evaluations'])
passcount = 0
for x in dq_data['evaluations']:
    if str(x['status']).upper() == 'PASS':
        passcount = passcount + 1

passpercentage =  int((passcount/count)*100)
dq_data['passingPercentage'] = passpercentage
dq_data['evaluationsCount'] = count
print(dq_data)

asset_id = get_asset_id(asset_name, DOMAIN_ID, PROJECT_ID )
res = client.get_form_type(
        domainIdentifier=DOMAIN_ID,
        formTypeIdentifier='amazon.datazone.DataQualityResultFormType'
    )
revision_id = res.get('revision')
# Post according to Boto3 documentation
response = client.post_time_series_data_points(
    domainIdentifier=DOMAIN_ID,
    entityIdentifier=asset_id,
    entityType='ASSET',
    forms=[{
        'formName': 'my_dq_results',
        'timestamp': datetime.now(timezone.utc),
        'content': json.dumps(dq_data),
        'typeIdentifier': 'amazon.datazone.DataQualityResultFormType',
        'typeRevision': revision_id
    }]
)

print(f"Success! Response: {response}")






response = client.list_asset_filters(
    assetIdentifier='string',
    domainIdentifier='string',
    maxResults=123,
    nextToken='string',
    status='VALID'|'INVALID'
)

response = client.get_listing(
    domainIdentifier=DOMAIN_ID,
    identifier='string',
    listingRevision='string'
)

response = client.search(
    domainIdentifier=DOMAIN_ID,
    searchScope='ASSET',
    SearchExpression={
        'Filters': [
            {
                'Name': 'assetName', # Most resources use 'Name' as suffix
                'Operator': 'Equals',
                'Value': 'stg_cust_new2'
            },
        ]
    },
    MaxResults=1
)


import boto3


# Simple usage
asset_id = get_asset_id('stg_cust_new2', 'dzd_3oxfuz0uw1456p', '5w2evsosqwud8h')
print(f"Asset ID: {asset_id}")
