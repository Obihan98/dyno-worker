def generate_codes(unique_codes):
    """
    Generate unique codes from the provided list of codes and return them in chunks of 250.
    
    Args:
        unique_codes (list): List of unique codes to be returned
        
    Returns:
        list: List of lists, where each inner list contains up to 250 unique codes
    """
    # Ensure we're working with a list
    if not isinstance(unique_codes, list):
        raise ValueError("unique_codes must be a list")
    
    # Remove any duplicates while preserving order
    seen = set()
    unique_codes_list = [x for x in unique_codes if not (x in seen or seen.add(x))]
    
    # Split into chunks of 250
    chunk_size = 250
    chunks = [unique_codes_list[i:i + chunk_size] for i in range(0, len(unique_codes_list), chunk_size)]
    
    return chunks 