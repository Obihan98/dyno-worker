import json
import logging
from processor.utils.db_connection import execute_query

# Get logger for this module
logger = logging.getLogger(__name__)

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
    params = {}
    
    if status is not None:
        update_parts.append("job_status = :status")
        params['status'] = status
    
    if response is not None:
        update_parts.append("job_response = :response")
        params['response'] = response
    
    if failed_codes is not None:
        update_parts.append("failed_codes = :failed_codes")
        params['failed_codes'] = json.dumps(failed_codes)
    
    if failed_codes_count is not None:
        update_parts.append("failed_code_count = :failed_codes_count")
        params['failed_codes_count'] = failed_codes_count
        
    if success_codes_count is not None:
        update_parts.append("success_code_count = :success_codes_count")
        params['success_codes_count'] = success_codes_count
        
    if current_batch is not None:
        update_parts.append("current_batch = :current_batch")
        params['current_batch'] = current_batch
    
    if s3_object_name is not None:
        update_parts.append("s3_object_name = :s3_object_name")
        params['s3_object_name'] = s3_object_name
    
    if not update_parts:
        return
    
    update_query = f"""
        UPDATE dyno_discounts 
        SET {', '.join(update_parts)}
        WHERE shop = :shop AND job_id = :job_id
    """
    
    # Add shop and job_id to params
    params['shop'] = shop
    params['job_id'] = int(job_id)
    
    logger.info(f"Updating job details for shop {shop}, job {job_id}")
    execute_query(update_query, params)

def get_job_details(shop, job_id):
    """
    Fetch the current row details for a specific job from the dyno_discounts table.
    
    Args:
        shop (str): The shop domain
        job_id (str): The job identifier
        
    Returns:
        dict: The row data if found, None otherwise
    """
    query = """
        SELECT *
        FROM dyno_discounts
        WHERE shop = :shop AND job_id = :job_id
    """
    
    params = {
        'shop': shop,
        'job_id': int(job_id)
    }
    
    logger.info(f"Fetching job details for shop {shop}, job {job_id}")
    result = execute_query(query, params)
    return result
