from processor.utils.gq_connection import execute_graphql
from processor.utils.db_connection import execute_query
from processor.utils.s3_connection import upload_file, download_file

from processor.code_generator.generator import generate_codes

import time
import json
import os
import csv
import re
import logging
from typing import List, Dict, Tuple, Any

# Get logger for this module
logger = logging.getLogger(__name__)

IS_DEV = os.getenv("IS_DEV")
MAX_RETRY_ATTEMPTS = 3

class CheckBulkCreationStatusError(Exception):
    """Exception raised when there are errors in bulk creation status check."""
    pass

class UploadCodesError(Exception):
    """Exception raised when there are errors in uploading codes."""
    pass

class CreateCSVError(Exception):
    """Exception raised when there are errors in creating CSV file."""
    pass

async def check_bulk_creation_status(shop, access_token, bulk_creation_id):
    gql_query = """
    query discountRedeemCodeBulkCreationStatus($id: ID!) {
        discountRedeemCodeBulkCreation(id: $id) {
            done
            codes (first: 250) {
                edges {
                    node {
                        code
                        discountRedeemCode {
                            code
                        }
                        errors {
                            message
                        }
                    }
                }
            }
        }
    }
    """

    variables = {
        "id": bulk_creation_id
    }
    gql_body = {
        "query": gql_query,
        "variables": variables
    }

    response = await execute_graphql(shop, access_token, gql_body)

    if response.get("errors"):
        raise CheckBulkCreationStatusError("Could not check bulk creation status. Discount may have been deleted or is invalid.")

    bulk_creation = response.get("data", {}).get("discountRedeemCodeBulkCreation", {})
    return bulk_creation.get("done", False), bulk_creation.get("codes", {}).get("edges", [])

def process_codes_status(codes_status):
    """
    Process the codes status from bulk creation and separate successful and unsuccessful codes.
    
    Args:
        codes_status (list): List of code status objects from the bulk creation response
        
    Returns:
        tuple: (successful_codes, unsuccessful_codes)
            - successful_codes: List of successfully created codes
            - unsuccessful_codes: List of dictionaries containing failed codes and their error messages
    """
    successful_codes = []
    unsuccessful_codes = []
    
    for code_status in codes_status:
        node = code_status.get("node", {})
        code = node.get("code")
        
        if node.get("discountRedeemCode") is not None:
            successful_codes.append(code)
        else:
            error_message = node.get("errors", [{}])[0].get("message", "Unknown error")
            unsuccessful_codes.append({
                "code": code,
                "error": error_message
            })
    
    return successful_codes, unsuccessful_codes

async def upload_codes(shop, access_token, code_chunk, discount_id):
    """
    Upload a single batch of discount codes to Shopify.
    
    Args:
        shop (str): The shop domain
        access_token (str): The access token for authentication
        code_chunk (list): List of codes to upload (max 250)
        discount_id (str): The Shopify discount ID
        
    Returns:
        tuple: (bool, list, list) - (success status, successful codes, unsuccessful codes)
    """
    # Format codes into the required input structure
    formatted_codes = [{"code": code} for code in code_chunk]
    
    # Create GraphQL mutation body
    gql_query = """
    mutation discountRedeemCodeBulkAdd($discountId: ID!, $codes: [DiscountRedeemCodeInput!]!) {
        discountRedeemCodeBulkAdd(discountId: $discountId, codes: $codes) {
            bulkCreation {
                id
            }
            userErrors {
                code
                field
                message
            }
        }
    }
    """

    variables = {
        "discountId": discount_id,
        "codes": formatted_codes
    }

    gql_body = {
        "query": gql_query,
        "variables": variables
    }
    
    response = await execute_graphql(shop, access_token, gql_body)

    # Check for errors in the response
    if not response or (response.get("errors") or response.get("data", {}).get("discountRedeemCodeBulkAdd", {}).get("userErrors")):
        raise UploadCodesError("Could not upload codes to Shopify. Please check the discount and try again." + str(response))

    bulk_creation_id = response.get("data", {}).get("discountRedeemCodeBulkAdd", {}).get("bulkCreation", {}).get("id")

    # Wait for 18 seconds not to overload the API
    time.sleep(18)

    job_done = False
    while not job_done:
        job_done, codes_status = await check_bulk_creation_status(shop, access_token, bulk_creation_id)
        time.sleep(3)

    successful_codes, unsuccessful_codes = process_codes_status(codes_status)

    return successful_codes, unsuccessful_codes

def update_job_details(shop, job_id, status=None, response=None, failed_codes=None, failed_codes_count=None, success_codes_count=None, current_batch=None, s3_object_name=None):
    """
    Update multiple job-related fields in the database in a single query.
    
    Args:
        shop (str): The shop domain
        job_id (str): The job identifier
        status (str, optional): The new status to set for the job
        response (str, optional): The response to set for the job
        failed_codes (list, optional): List of failed codes to store
        failed_codes_count (int, optional): Count of failed codes
        success_codes_count (int, optional): Count of successful codes
        current_batch (int, optional): The current batch number being processed
    """
    update_parts = []
    params = []
    
    if status is not None:
        update_parts.append("job_status = %s")
        params.append(status)
    
    if response is not None:
        update_parts.append("job_response = %s")
        params.append(response)
    
    if failed_codes is not None:
        update_parts.append("failed_codes = %s")
        params.append(json.dumps(failed_codes))
    
    if failed_codes_count is not None:
        update_parts.append("failed_code_count = %s")
        params.append(failed_codes_count)
        
    if success_codes_count is not None:
        update_parts.append("success_code_count = %s")
        params.append(success_codes_count)
        
    if current_batch is not None:
        update_parts.append("current_batch = %s")
        params.append(current_batch)
    
    if s3_object_name is not None:
        update_parts.append("s3_object_name = %s")
        params.append(s3_object_name)
    
    if not update_parts:
        return
    
    update_query = f"""
        UPDATE dyno_discounts 
        SET {', '.join(update_parts)}
        WHERE shop = %s AND job_id = %s
    """
    
    params.extend([shop, int(job_id)])
    
    logger.info(f"Updating job details for shop {shop}, job {job_id}")
    execute_query(update_query, tuple(params))

async def retry_failed_codes(
    shop: str,
    access_token: str,
    discount_id: str,
    unsuccessful_codes: List[Dict[str, str]],
    discount_created: Dict[str, Any]
) -> Tuple[List[str], List[Dict[str, str]]]:
    """Retry generation and upload of failed codes."""
    retry_count = 0
    remaining_unsuccessful = unsuccessful_codes
    all_successful = []
    
    while remaining_unsuccessful and retry_count < MAX_RETRY_ATTEMPTS:
        retry_count += 1
        logger.info(f"Retry attempt {retry_count}/{MAX_RETRY_ATTEMPTS} for {len(remaining_unsuccessful)} unsuccessful codes...")
        
        retry_codes = generate_codes(discount_created, len(remaining_unsuccessful))
        _, retry_successful, retry_unsuccessful = await process_discount_codes(
            shop, access_token, retry_codes, discount_id, discount_created['job_id']
        )
        
        if retry_successful:
            logger.info(f"Successfully retried {len(retry_successful)} codes in attempt {retry_count}")
            all_successful.extend(retry_successful)
        
        remaining_unsuccessful = retry_unsuccessful
        
        if remaining_unsuccessful:
            logger.info(f"Still have {len(remaining_unsuccessful)} unsuccessful codes after attempt {retry_count}")
        else:
            logger.info("All codes successfully generated!")
            break
    
    if remaining_unsuccessful:
        logger.warning(f"Failed to generate {len(remaining_unsuccessful)} codes after {MAX_RETRY_ATTEMPTS} attempts")
    
    return all_successful, remaining_unsuccessful

def sanitize_filename(filename: str) -> str:
    """
    Sanitize a string to be used as a filename by removing or replacing unsafe characters.
    Only allows numbers and alphanumerical characters.
    
    Args:
        filename (str): The original filename
        
    Returns:
        str: A sanitized filename safe for use in file systems
    """
    # Replace unsafe characters with empty string
    sanitized = re.sub(r'[^a-zA-Z0-9]', '', filename)
    # Ensure the filename isn't empty
    if not sanitized:
        sanitized = "discountcodes"
    return sanitized

def delete_temp_file(file_path: str) -> None:
    """
    Delete a temporary file from the filesystem.
    
    Args:
        file_path (str): Path to the file to be deleted
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Successfully deleted temporary file: {file_path}")
    except Exception as e:
        logger.error(f"Error deleting temporary file {file_path}: {str(e)}")

def download_and_read_csv(s3_object_name: str) -> List[str]:
    """
    Download and read a CSV file from S3 using streaming to minimize memory usage.
    
    Args:
        s3_object_name (str): The S3 object name/URL of the CSV file
        
    Returns:
        List[str]: List of codes from the CSV file
        
    Raises:
        CreateCSVError: If there are any issues downloading, reading, or processing the CSV file
    """
    local_path = None
    try:
        # Create temp directory if it doesn't exist
        temp_dir = "temp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        # Create a unique filename for the downloaded file
        filename = os.path.basename(s3_object_name)
        local_path = os.path.join(temp_dir, filename)
        
        # Download the file from S3
        success = download_file(s3_object_name, local_path)
        if not success:
            raise CreateCSVError("Failed to download file from S3")

        # Read the CSV file using streaming
        codes = []
        with open(local_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            try:
                next(reader)  # Skip header row
            except StopIteration:
                raise CreateCSVError("CSV file is empty or has no header row")
            
            # Process rows in chunks to manage memory
            chunk_size = 1000
            current_chunk = []
            
            for row in reader:
                if not row:  # Skip empty rows
                    continue
                code = row[0].strip()
                if code:  # Only add non-empty codes
                    current_chunk.append(code)
                    
                    # Process chunk when it reaches the size limit
                    if len(current_chunk) >= chunk_size:
                        codes.extend(current_chunk)
                        current_chunk = []
            
            # Add any remaining codes
            if current_chunk:
                codes.extend(current_chunk)

        if not codes:
            raise CreateCSVError("No valid codes found in CSV file")

        return codes
    except Exception as e:
        if IS_DEV: print(f"Error downloading/reading CSV file: {str(e)}", flush=True)
        raise CreateCSVError(f"Error downloading/reading CSV file: {str(e)}")
    finally:
        # Clean up temporary file
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception as e:
                if IS_DEV: print(f"Error cleaning up temporary file: {str(e)}", flush=True)

def create_and_upload_codes_csv(task_name, discount_created: Dict[str, Any], all_successful_codes: List[str], discount_title: str, discount_id: str, initial_code: str, s3_object_name: str) -> str:
    """
    Create a CSV file with generated codes and upload it to S3.
    
    Args:
        discount_created (dict): Dictionary containing discount creation parameters
        all_successful_codes (list): List of successfully generated codes
        
    Returns:
        str: The S3 object name/URL of the uploaded file
    """
    try:
        if task_name != "initialCodeGeneration":
            # Download the CSV file from S3
            old_codes = download_and_read_csv(s3_object_name)
            all_successful_codes = old_codes + all_successful_codes

        temp_dir = "temp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        sanitized_title = sanitize_filename(discount_title)
        csv_filename = f"discount_codes_{sanitized_title}_{discount_id.split('/')[-1]}.csv"
        csv_path = os.path.join(temp_dir, csv_filename)
        
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['code'])  # Header
            if initial_code != "":
                writer.writerow([initial_code])
            for code in all_successful_codes:
                writer.writerow([code])

        # Upload codes to S3 and get URL
        object_name = upload_file(csv_path, csv_filename)
        
        # Delete the temporary file after successful upload
        delete_temp_file(csv_path)
        
        return object_name
    except Exception as e:
        # Clean up the temporary file if it exists
        if 'csv_path' in locals():
            delete_temp_file(csv_path)
        raise CreateCSVError(f"Error creating/uploading CSV file: {str(e)}")

async def process_discount_codes(task_name, shop, access_token, discount_created, discount_id, job_id, discount_title, s3_object_name):
    """
    Generate and upload discount codes to the Shopify Admin API using streaming to minimize memory usage.
    
    Args:
        shop (str): The shop domain
        access_token (str): The access token for authentication
        discount_created (dict): Dictionary containing discount creation parameters
        discount_id (str): The Shopify discount ID in the format 'gid://shopify/DiscountCodeNode/{id}'
        job_id (str): The job identifier
    
    Returns:
        tuple: (bool, list, list) - (success status, all successful codes, all unsuccessful codes)
    """
    # Initialize counters instead of storing all codes in memory
    total_successful = 0
    total_unsuccessful = 0
    failed_codes = []
    total_codes = 0

    try:
        # Generate codes in batches
        code_batches = generate_codes(task_name, discount_created)
        total_codes = sum(len(chunk) for chunk in code_batches) + 1 if task_name == "initialCodeGeneration" else sum(len(chunk) for chunk in code_batches)

        logger.info(f"Creating {total_codes} discount codes for {shop}")
        time.sleep(2)
        update_job_details(shop, job_id, status="generating_codes")

        # Process each batch and write to S3 immediately
        temp_dir = "temp"
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            
        sanitized_title = sanitize_filename(discount_title)
        csv_filename = f"discount_codes_{sanitized_title}_{discount_id.split('/')[-1]}.csv"
        csv_path = os.path.join(temp_dir, csv_filename)
        
        # Open CSV file for writing
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['code'])  # Header
            
            # Write initial code if present
            if task_name == "initialCodeGeneration" and discount_created.get('initialCode'):
                writer.writerow([discount_created['initialCode']])
                total_successful += 1

            # Process each batch
            for batch_num, code_chunk in enumerate(code_batches, 1):
                successful_codes, unsuccessful_codes = await upload_codes(shop, access_token, code_chunk, discount_id)
                
                # Write successful codes to CSV immediately
                for code in successful_codes:
                    writer.writerow([code])
                total_successful += len(successful_codes)
                
                # Store failed codes (limited to last 1000 for memory management)
                failed_codes.extend(unsuccessful_codes)
                if len(failed_codes) > 1000:
                    failed_codes = failed_codes[-1000:]
                total_unsuccessful += len(unsuccessful_codes)

                update_job_details(shop, job_id, current_batch=batch_num)

        # Upload the CSV file to S3
        s3_object_name = upload_file(csv_path, csv_filename)
        
        # Clean up temporary file
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # Handle retries for failed codes if needed
        if failed_codes and discount_created['style'] == 'random':
            retry_successful, remaining_unsuccessful = await retry_failed_codes(
                shop, access_token, discount_id, failed_codes, discount_created
            )
            total_successful += len(retry_successful)
            total_unsuccessful = len(remaining_unsuccessful)
            failed_codes = remaining_unsuccessful

        # Update final job status
        update_job_details(
            shop,
            job_id,
            status="codes_generated_some_failed" if total_unsuccessful > 0 else "codes_generated",
            response="Failed to generate some codes" if total_unsuccessful > 0 else "All codes generated successfully",
            failed_codes=failed_codes,
            success_codes_count=total_successful,
            failed_codes_count=total_unsuccessful,
            s3_object_name=s3_object_name
        )
        
        return True
    
    except CheckBulkCreationStatusError as e:
        logger.error(f"Error processing discount codes for shop {shop}, job {job_id}: {str(e)}")
        update_job_details(
            shop,
            job_id,
            status="code_generation_failed",
            failed_codes=f"Error processing discount codes: {str(e)}",
            failed_codes_count=total_codes,
            success_codes_count=0,
        )
        return False
    except UploadCodesError as e:
        logger.error(f"Error processing discount codes for shop {shop}, job {job_id}: {str(e)}")
        update_job_details(
            shop,
            job_id,
            status="code_generation_failed",
            failed_codes=f"Error processing discount codes: {str(e)}",
            failed_codes_count=total_codes,
            success_codes_count=0,
        )
        return False
    except Exception as e:
        logger.error(f"Error processing discount codes for shop {shop}, job {job_id}: {str(e)}")
        update_job_details(
            shop,
            job_id,
            status="code_generation_failed",
            failed_codes=f"Error processing discount codes: {str(e)}",
            failed_codes_count=total_codes,
            success_codes_count=0,
        )  
        return False

