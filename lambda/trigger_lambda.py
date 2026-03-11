import json
import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    
    job_run = glue_client.start_job_run(
        JobName='TSLA Pipeline'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('pipeline Triggered')
    }