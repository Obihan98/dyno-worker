import random

def generate_codes(allow_letters, allow_numbers, len_codes, prefix, suffix, n_codes, disallowed_characters=None):
    """
    Generate unique discount codes based on specified parameters.
    
    Args:
        allow_letters (bool): Whether to include letters in codes
        allow_numbers (bool): Whether to include numbers in codes
        len_codes (int): Length of the random part of the code
        prefix (str): Prefix to add before the random code
        suffix (str): Suffix to add after the random code
        n_codes (int): Number of unique codes to generate
        disallowed_characters (list): List of characters to exclude from code generation
        
    Returns:
        List of lists, where each inner list contains maximum 250 codes
    """
    characters = []
    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    numbers = '0123456789'

    if allow_letters:
        characters.extend(letters)

    if allow_numbers:
        characters.extend(numbers)

    # Remove disallowed characters from the character set
    if disallowed_characters:
        characters = [c for c in characters if c not in disallowed_characters]

    if not characters:
        raise ValueError("No valid characters available for code generation after applying restrictions")

    def generate_random_code():
        """Generate a single random code based on the character set."""
        return ''.join(random.choice(characters) for _ in range(len_codes))

    discount_codes = set()
    while len(discount_codes) < n_codes:
        code = generate_random_code()
        new_code = f"{prefix}{code}{suffix}"
        discount_codes.add(new_code)

    # Convert set to list and split into chunks of 250
    codes_list = list(discount_codes)
    chunked_codes = [codes_list[i:i + 250] for i in range(0, len(codes_list), 250)]
    
    return chunked_codes
