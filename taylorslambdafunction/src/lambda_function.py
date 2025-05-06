import boto3
import os
import uuid
from urllib.parse import unquote_plus

s3_client = boto3.client(
    's3',
    aws_access_key_id="AKIAW3MD7R6N6TKPSWXZ",
    aws_secret_access_key="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"
)

def convert_to_uppercase(file_path, output_path):
    with open(file_path, 'r') as file:
        content = file.read()
    content_upper = content.upper()
    with open(output_path, 'w') as file:
        file.write(content_upper)

def lambda_handler(event, context):
    print('Begin processing text file')
    for record in event['Records']:
        source_bucket = 'taylorssourcebucket'
        key = unquote_plus(record['s3']['object']['key'])
        print(f'Source bucket: {source_bucket}')
        print(f'Key: {key}')
        tmpkey = key.replace('/', '')
        download_path = f'/tmp/{uuid.uuid4()}{tmpkey}'
        upload_path = f'/tmp/uppercase-{tmpkey}'
        
        try:
            # Download the original text file
            s3_client.download_file(source_bucket, key, download_path)
            print(f'Downloaded file to {download_path}')
            
            # Convert the content to uppercase
            convert_to_uppercase(download_path, upload_path)
            print(f'Converted file to uppercase and saved to {upload_path}')
            
            # Read and print the content of the converted file
            with open(upload_path, 'r') as file:
                output_content = file.read()
                print('Output file content:')
                print(output_content)
            
            # Upload the uppercase text file to the destination bucket
            destination_bucket = 'taylorsdestinationbucket'
            destination_key = f'destination-folder/uppercase-{tmpkey}'
            s3_client.upload_file(upload_path, destination_bucket, destination_key)
            
            print(f'Text file processing complete and uploaded to {destination_bucket}/{destination_key}')
            
        except Exception as e:
            print(f"Error processing file: {e}")
            raise e
