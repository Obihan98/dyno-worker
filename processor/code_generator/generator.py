from . import random, specific, import_codes

def generate_codes(task_name, discount_created, retry_count=0):
    """
    Generate discount codes based on the specified generator type.
    
    Args:
        discount_created (dict): Dictionary containing discount creation parameters
        
    Returns:
        List of generated discount codes
    """
    code_config = discount_created['code']
    generator_type = discount_created['style']
    num_codes = retry_count if task_name == "retry" else code_config['count'] - 1 if task_name == "initialCodeGeneration" else code_config['count']
    
    match generator_type:
        case 'random':
            return random.generate_codes(
                allow_letters=code_config['allow_letters'],
                allow_numbers=code_config['allow_numbers'],
                len_codes=code_config['length'],
                prefix=code_config['prefix'],
                suffix=code_config['suffix'],
                n_codes=num_codes
            )
        case 'specific':
            return specific.generate_codes(task_name, code_config['specific'])
        case 'import':
            return import_codes.generate_codes(
                unique_codes=code_config['import']['uniqueCodes'][1:] if task_name == "initialCodeGeneration" else code_config['import']['uniqueCodes']
            )
        case _:
            raise ValueError(f"Invalid generator type: {generator_type}")