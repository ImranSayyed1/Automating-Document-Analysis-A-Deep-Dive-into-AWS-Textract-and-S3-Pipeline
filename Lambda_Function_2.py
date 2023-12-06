#Lambda Function 2

import json
import boto3

def lambda_handler(event, context):
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Source and destination bucket names and folder
    source_bucket_name = 'bill-data-project-retail-test'
    destination_bucket_name = 'json-clean-data'

    try:
        # List all objects (files) in the source S3 bucket
        objects = s3_client.list_objects(Bucket=source_bucket_name)
        cnt = 1
        # Iterate through each object in the bucket
        for obj in objects.get('Contents', []):
            json_file_key = obj['Key']

            # Print the file path for debugging
            # print(f"File path -> {json_file_key}")

            # Skip files that are not JSON or already processed
            if json_file_key.endswith('.json') and json_file_key.split('/')[-2].lower() == 'textract':
                # print(cnt)
                # cnt += 1
                # Extract session_id from the file path
                session_id = json_file_key.split('/')[-4]
                # print(f"Session ID -> {session_id}")

                # Extract store_id from the file path
                store_id = json_file_key.split('/')[-6]
                # print(f"Store ID > {store_id}")
                
                # Extract tenant_id from file path
                tenant_id = json_file_key.split('/')[-8]
                # print(f"Tenant ID > {tenant_id}")
                
                # Get the JSON file from the source S3 bucket
                response = s3_client.get_object(Bucket=source_bucket_name, Key=json_file_key)
                json_content = response['Body'].read().decode('utf-8')

                try:
                    # Parse the JSON content into a Python dictionary
                    python_dict = json.loads(json_content)
                    # print(json_file_key)

                    # Extract the "items," "price," and "quantity" arrays from "lineitems" and add them to "kv_data"
                    lineitems = json.loads(python_dict['lineitems'])
                    kv_data = json.loads(python_dict['kv_data'])
                    if 'total_amount' in python_dict:
                        total_amount = python_dict['total_amount']
                    else:
                        total_amount = None  # Assign a null value

                    kv_data['Type'].extend(['items', 'price', 'quantity', 'tenant_ids', 'session_ids', 'store_ids', 'total_amount'])
                    kv_data['Key'].extend(['items', 'price', 'quantity', 'tenant_ids', 'session_ids', 'store_ids', 'total_amount'])
                    kv_data['Value'].extend([lineitems['items'], lineitems['price'], lineitems['quantity'], tenant_id, session_id, store_id, total_amount])

                    
                    # Specify the destination key for the updated JSON file (maintaining the same structure)
                    json_file_key = json_file_key.split('/')[-1]
                    # print(json_file_key)
    
                    # print("total amount", total_amount)
                    # print(kv_data['Key'])
                    # print(kv_data['Type'])
                    # print(kv_data['Value'])
                    
                    for i, e in enumerate(kv_data['Type']) :
                        if 'DATE' in e.upper():
                            # print(e)
                            kv_data['Type'][i] = 'DATE'
                            # print(kv_data['Type'])
                            break
                    
                    # Update the "kv_data" field in the Python dictionary
                    python_dict['kv_data'] = json.dumps(kv_data)
                    python_dict['session_id'] = session_id
                    python_dict['store_id'] = store_id
                    python_dict['tenant_id'] = tenant_id
                    
                    # Serialize the updated Python dictionary back to JSON
                    updated_json_content = json.dumps(python_dict)
                    
                    # Upload the updated JSON content to the destination S3 location with the same structure
                    s3_client.put_object(Bucket=destination_bucket_name, Key=json_file_key, Body=updated_json_content)
                    # print(updated_json_content)

    
                except json.JSONDecodeError as e:
                    # Handle JSON decoding errors
                    print(f"Error decoding JSON for file {json_file_key}: {str(e)}")
                    print(f"JSON content for file {json_file_key}: {json_content}")
                    
                    
            else:
                continue
    
        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed for all JSON files in the bucket')
            }
    except Exception as e:
        # Handle any other exceptions
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing JSON files in the bucket')
        }
    # print(json_file_key)
    # job_names = ['KV_and_lineitem_extractions', 'KV_new']
    # jc = 1
        
    # for job_name in job_names:
    #     jc = jc+1
    #     print(jc, " job started")
    #     response = glue.start_job_run(JobName=job_name)
    #     print(f'Started Glue job {job_name} with JobRunId: {response["JobRunId"]}')
