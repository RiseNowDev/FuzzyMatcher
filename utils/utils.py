from rapidfuzz import fuzz, process
import re
import logging, tqdm

# Custom weighted ratio function combining multiple string similarity metrics
def weighted_ratio(s1, s2, score_cutoff=0):
    """
    Calculates the weighted ratio between two strings.

    Args:
        s1 (str): The first string.
        s2 (str): The second string.
        score_cutoff (int, optional): The minimum score required for a match. Defaults to 0.

    Returns:
        float: The weighted ratio score between the two strings.
    """
    token_sort_ratio = fuzz.token_sort_ratio(s1, s2)
    token_set_ratio = fuzz.token_set_ratio(s1, s2)
    partial_ratio = fuzz.partial_ratio(s1, s2)

    # Calculate weighted average
    weighted_score = (0.4 * token_sort_ratio +
                      0.4 * token_set_ratio +
                      0.2 * partial_ratio)

    return weighted_score

# Function to normalize names by removing digits, special characters, and converting to lowercase
def normalize_name(name: str) -> str:
    """
    Normalize a given name by removing digits, replacing special characters with space,
    converting to lowercase, removing extra whitespace, and joining words.

    Args:
        name (str): The name to be normalized.

    Returns:
        str: The normalized name.
    """
    if not name:
        return ''
    name = re.sub(r'\d+', '', name)  # Remove digits
    name = re.sub(r'[^\w\s]', ' ', name)  # Replace special characters with space
    name = name.lower()  # Convert to lowercase
    return ' '.join(name.split())  # Remove extra whitespace and join words

# Function to find matches and scores for a chunk of data
def find_matches_and_scores(args: tuple) -> list:
    """
    Find matches and scores for a given chunk of data.

    Args:
        args (tuple): A tuple containing the chunk of data, all normalized names, and the percentage score.

    Returns:
        list: A list of matches and their scores.

    """
    chunk, all_normalized_names, percentage_score = args
    matches = []
    for idx, row in tqdm(chunk.iterrows(), total=len(chunk), desc="Processing chunk", leave=False):
        normalized_name = row['normalized_name']
        results = process.extract(
            normalized_name,
            all_normalized_names,
            scorer=weighted_ratio,
            limit=100
        )
        
        # Log the results of the matching
        logging.debug(f"Matching results for '{normalized_name}': {results}")

        # Filter results based on percentage_score during extraction
        filtered_results = [match for match in results if match[1] >= percentage_score and row['i'] < match[2]]
        if not filtered_results:
            logging.debug(f"No matches found for '{normalized_name}' with score >= {percentage_score}")
        else:
            matches.extend((row['i'], normalized_name, all_normalized_names[match[2]], match[2], match[1], row['source'])
                           for match in filtered_results)
    return matches