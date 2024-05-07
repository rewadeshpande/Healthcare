import boto3
import os
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    comprehend_medical_client = boto3.client('comprehendmedical', region_name='us-east-2')
    s3_client = boto3.client('s3')

    results = []
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        try:
            file_obj = s3_client.get_object(Bucket=bucket, Key=key)
            extracted_text = file_obj['Body'].read().decode('utf-8')
            
            # Handle large documents by splitting them into chunks
            max_size = 20000  # Comprehend Medical size limit
            chunks = [extracted_text[i:i + max_size] for i in range(0, len(extracted_text), max_size)]
            processed_text = ""
            
            for chunk in chunks:
                phi_response = comprehend_medical_client.detect_phi(Text=chunk)
                for entity in phi_response['Entities']:
                    placeholder = '[PHI]'
                    if entity['Type'] in ['DATE', 'AGE', 'TIME']:
                        placeholder = '[DATE]'
                    elif entity['Type'] in ['NAME', 'PROFESSION']:
                        placeholder = '[NAME]'
                    chunk = chunk.replace(entity['Text'], placeholder)
                processed_text += chunk  # Concatenate processed chunks

            file_base_name = os.path.splitext(os.path.basename(key))[0]
            anonymized_key = f"anonymized_output/anonymized_{file_base_name}.txt"
            
            s3_client.put_object(Bucket=bucket, Key=anonymized_key, Body=processed_text)
            results.append(f"Processed and stored {anonymized_key}")
        except Exception as e:
            results.append(f"Failed to process {key}: {str(e)}")

    return {
        'statusCode': 200,
        'body': '\n'.join(results)
    }


