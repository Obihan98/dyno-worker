from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get environment variables
IS_DEV = os.getenv("IS_DEV")
S3_CREDENTIALS = os.getenv('S3_CREDENTIALS')
s3_bucketName, s3_region, s3_accessKeyId, s3_secretAccessKey = S3_CREDENTIALS.split(':')

# Initialize S3 client
s3 = boto3.client(
    's3',
    region_name=s3_region,
    aws_access_key_id=s3_accessKeyId,
    aws_secret_access_key=s3_secretAccessKey
)

def generate_url(object_name):
    """
    Generate a presigned URL for an S3 object that is valid for 1 day
    
    Args:
        object_name (str): The name of the S3 object
        
    Returns:
        str: Presigned URL for the object, valid for 1 day
    """
    try:
        # Generate URL with explicit parameters
        url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': s3_bucketName,
                'Key': object_name,
                'ResponseContentDisposition': 'attachment'  # Force download
            },
            ExpiresIn=86400,  # 1 day in seconds
            HttpMethod='GET'
        )
        
        # Parse the URL to ensure it's valid
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Generated URL is invalid")
            
        return url
    except ClientError as e:
        logger.error(f"Error generating URL: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error generating URL: {e}")
        return None

def upload_file(file_path, object_name):
    """
    Upload a file to S3 and return the object name and URL
    
    Args:
        file_path (str): Path to the file to upload
        object_name (str): Name to give the file in S3
        
    Returns:
        tuple: (object_name, url)
            - object_name: The name of the object in S3
            - url: Presigned URL valid for 1 day
    """
    try:
        # Upload file with content disposition for download
        s3.upload_file(
            file_path, 
            s3_bucketName, 
            object_name,
            ExtraArgs={
                'ContentDisposition': 'attachment'
            }
        )
            
        return object_name
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Unexpected error in upload_file: {e}")
        return None, None

def download_file(object_name, local_path):
    """
    Download a file from S3 to a local path
    
    Args:
        object_name (str): Name of the object in S3 to download
        local_path (str): Local path where the file should be saved
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    try:
        s3.download_file(
            s3_bucketName,
            object_name,
            local_path
        )
        return True
    except ClientError as e:
        logger.error(f"Error downloading file from S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in download_file: {e}")
        return False
