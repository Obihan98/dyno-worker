import random

def generate_codes(task_name, specific_codes):
    # Split codes by newline and strip whitespace
    codes = [code.strip() for code in specific_codes.split('\n')]

    # Remove the first code from the list
    if task_name == "initialCodeGeneration" and codes:
        codes = codes[1:]
    
    # Remove duplicates by converting to set and back to list
    codes = list(set(codes))
    
    # Create batches of 250 codes
    batch_size = 250
    batches = [codes[i:i + batch_size] for i in range(0, len(codes), batch_size)]
    
    return batches
