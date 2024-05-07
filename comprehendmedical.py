import boto3
import urllib.parse

def redact_text(original_text, entities):
    """Apply redactions to the original text based on entity offsets."""
    sorted_entities = sorted(entities, key=lambda e: e['BeginOffset'], reverse=True)
    for entity in sorted_entities:
        mask = 'X' * (entity['EndOffset'] - entity['BeginOffset'])
        original_text = original_text[:entity['BeginOffset']] + mask + original_text[entity['EndOffset']:]
    return original_text

def split_text(text, max_size=20000):
    """ Split text into chunks. """
    chunks = []
    current_chunk = []
    current_size = 0
    words = text.split()
    for word in words:
        word_size = len(word.encode('utf-8')) + 1  # +1 for space
        if current_size + word_size > max_size:
            chunks.append(' '.join(current_chunk))
            current_chunk = [word]
            current_size = word_size
        else:
            current_chunk.append(word)
            current_size += word_size
    if current_chunk:  # add the last chunk
        chunks.append(' '.join(current_chunk))
    return chunks

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    comprehend_medical_client = boto3.client('comprehendmedical', region_name='us-east-2')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    output_prefix = 'anonymized-output/'

    try:
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        text = file_obj['Body'].read().decode('utf-8')
        chunks = split_text(text)
        all_entities = []
        offset = 0

        for chunk in chunks:
            phi_response = comprehend_medical_client.detect_phi(Text=chunk)
            medical_entities_response = comprehend_medical_client.detect_entities_v2(Text=chunk)
            entities = phi_response['Entities'] + medical_entities_response['Entities']

            # Adjust entity offsets and add to all_entities
            for entity in entities:
                entity['BeginOffset'] += offset
                entity['EndOffset'] += offset
                all_entities.append(entity)

            offset += len(chunk) + 1  # +1 for the space that was at the end of the chunk

        # Redact PHI and other medical entities from the text
        anonymized_text = redact_text(text, all_entities)

        # Store the anonymized text in the new directory of the same bucket
        new_key = output_prefix + key.split('/')[-1]
        s3_client.put_object(Body=anonymized_text, Bucket=bucket, Key=new_key)

    except Exception as e:
        print(e)
        raise e

