import io

import boto3
from PIL import Image

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # Get the source bucket and key from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    # Extract the filename and extension
    file_name = source_key.split('/')[-1]
    file_name_wo_extension, file_extension = file_name.split('.')
    file_extension = file_extension.lower()

    available_extensions = ['jpg', 'jpeg']

    if file_extension not in available_extensions:
        return {
            'statusCode': 500,
            'body': f'Error converting image: ".{file_extension}" is not available'
        }

    prefix_directory = 'images'

    # Define the output directories
    output_directories = {
        'bmp': f'{prefix_directory}/bmp/',
        'gif': f'{prefix_directory}/gif/',
        'png': f'{prefix_directory}/png/'
    }

    try:
        # Load the image from S3
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        image_data = response['Body'].read()
        image = Image.open(io.BytesIO(image_data))

        converted_images = []

        # Convert the image to each desired format
        for format_type, output_directory in output_directories.items():
            output_key = output_directory + file_name_wo_extension + '.' + format_type

            # Save the converted image to a buffer
            output_buffer = io.BytesIO()
            image.save(output_buffer, format=format_type.upper())
            output_buffer.seek(0)

            # Upload the converted image to S3
            s3_client.put_object(Body=output_buffer, Bucket=source_bucket, Key=output_key)

            converted_images.append(output_key)

        return {
            'statusCode': 200,
            'body': f'Image conversion successful. Converted images: {", ".join(converted_images)}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error converting image: {str(e)}'
        }
