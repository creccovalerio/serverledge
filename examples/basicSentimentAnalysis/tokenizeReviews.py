import re
import json

def handler(params, context):
    return tokenizeReviews(params["input"])

def tokenizeReviews(reviews):
    """
    Tokenizes reviews provided as a JSON-encoded string.
    - Converts the JSON string to a Python list
    - Splits each review into individual words (tokens)
    """
    # Decode the JSON string into a Python list
    reviews = json.loads(reviews)
    
    tokenized_reviews = []
    for review in reviews:
        review = review.lower()  # Convert to lowercase
        review = re.sub(r'[^\w\s]', '', review)  # Remove punctuation
        tokens = review.split()  # Split into tokens (words)
        tokenized_reviews.append(tokens)
    
    return tokenized_reviews
