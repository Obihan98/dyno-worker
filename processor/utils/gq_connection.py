import aiohttp
import logging
import time
import asyncio
from typing import Dict, Any

# Get logger for this module
logger = logging.getLogger(__name__)

async def execute_graphql(shop, access_token, query):
    """
    Execute a GraphQL query against the Shopify Admin API.
    
    Args:
        shop (str): The shop's domain (e.g., 'your-store.myshopify.com')
        access_token (str): The Shopify access token
        query (Dict[str, Any]): The GraphQL query to execute
        
    Returns:
        Dict[str, Any]: The parsed JSON response from the API
        
    Raises:
        Exception: If there's an error during the API call
    """
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            url = f"https://{shop}/admin/api/2025-01/graphql.json"
            headers = {
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": access_token
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=query) as response:
                    if response.status == 502:
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"Received 502 error, retrying in 5 seconds... (Attempt {retry_count}/{max_retries})")
                            await asyncio.sleep(5)
                            continue
                    
                    return await response.json()
                    
        except Exception as error:
            logger.error(f"GraphQL Execution Error: {error}")
            raise error
